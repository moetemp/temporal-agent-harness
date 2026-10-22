# ABOUTME: How the harness reaches its Streams: the one place the Stream plugin is built, and the
# readers every client-side consumer of turn events shares. The plugin is registered once on the
# client. Workflow code creates and writes the Stream (``AgentWorkflowRunner``), activities get it
# in their input and write it through ``BufferedPublisher``, and clients read it through
# ``StreamClient``.
#
# An agent tree shares ONE Stream: the one the root agent creates (``AgentEvents``). Each agent
# publishes on its own Topic (``turn_events`` for the root, ``subagent_topic(agent_id)`` below it),
# so one read of the Stream gives every agent's events in one order, and a Topic filter gives one
# agent's.

from __future__ import annotations

import dataclasses
import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.streams import (
    ClientStream,
    ClientTopic,
    Closed,
    Record,
    StreamClient,
    StreamNotFoundError,
    StreamOffsetError,
    StreamPlugin,
    Superseded,
)
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.contrib.streams.testing import MemoryStreamPlugin

from temporal_agent_harness.harness.agent_protocol import (
    AGENT_STREAM_MEMO,
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AgentEvents,
    AgentStream,
    AttemptSuperseded,
    subtree_prefix,
)

STORE_ENV = "STREAMS_STORE"
REDIS_URL_ENV = "STREAMS_REDIS_URL"
KEY_PREFIX_ENV = "STREAMS_KEY_PREFIX"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379"
# How long a read waits for a Stream that does not exist yet. The root creates its Stream in its
# first Workflow Task, and a client that just started the root may read before that.
_WAIT_FOR_CREATE = timedelta(hours=1)
# Every client of this process shares one memory plugin, so they all see the same Streams.
_memory: MemoryStreamPlugin | None = None


class TurnCursorError(ValueError):
    """A turn cursor that is not a position on the agent's Stream."""


def streams_from_env() -> StreamPlugin:
    """The Stream plugin that ``STREAMS_STORE`` names.

    ``redis`` (the default) is the Redis at ``STREAMS_REDIS_URL``. The web app, the Nexus adapter
    and the workers are separate processes, and they must all reach one Redis.
    ``STREAMS_KEY_PREFIX`` puts the keys under a prefix of their own, which keeps two deployments
    (or two test runs) on one Redis apart. ``memory`` keeps the Streams in this process, for tests
    and a single process only.

    Register the result once, as ``Client.connect(..., plugins=[streams])``. Workers built from
    that client take it, workflow code and activities reach it through their Stream handles, and
    any other code through ``StreamClient(client)``. One plugin serves one client connection, so a
    second connection needs a plugin of its own, except the memory plugin, which all of them
    share.
    """
    global _memory
    store = os.environ.get(STORE_ENV, "redis")
    if store == "memory":
        if _memory is None:
            _memory = MemoryStreamPlugin()
        return _memory
    if store != "redis":
        raise ValueError(f"{STORE_ENV}={store!r} names no Stream store")
    return RedisStreamPlugin(
        os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL),
        key_prefix=os.environ.get(KEY_PREFIX_ENV) or None,
    )


def parse_cursor(cursor: str) -> int | None:
    """The offset a stored cursor names, or None for the beginning.

    A cursor is the offset to read from next, in decimal. Offsets are above zero, so zero is the
    beginning too.
    """
    if not cursor:
        return None
    try:
        offset = int(cursor)
    except ValueError:
        raise TurnCursorError(f"not a turn cursor: {cursor!r}") from None
    if offset < 0:
        raise TurnCursorError(f"not a turn cursor: {cursor!r}")
    return offset or None


def cursor_after(item: Record[Any]) -> str:
    """The cursor that resumes just past ``item``."""
    return str(item.offset + 1)


@dataclass(frozen=True)
class TurnStream:
    """The Stream an agent publishes on, and the Topic that is its own."""

    stream: ClientStream[AgentEvents]
    topic: str
    # How long a read waits for the Stream to exist. None reads at once.
    create_wait: timedelta | None = None

    def turn_events(self) -> ClientTopic[AgentEvent]:
        """The agent's own Topic, typed."""
        return self.stream.topic(self.topic, type=AgentEvent)


async def turn_stream(client: Client, workflow_id: str, *, wait: bool = True) -> TurnStream:
    """The Stream and Topic of the agent ``workflow_id``. ``client`` carries the Stream plugin.

    A top-level agent publishes on the Stream it creates. A subagent publishes on its root's,
    and its memo carries the root's Stream handle, so a client that knows only the subagent
    finds the Stream with one Describe call. With ``wait``, a read of a Stream that does not
    exist yet waits for it. The read ends when the root's run chain closes the Stream.
    """
    streams = StreamClient(client)
    create_wait = _WAIT_FOR_CREATE if wait else None
    description = await client.get_workflow_handle(workflow_id).describe()
    located = await description.memo_value(AGENT_STREAM_MEMO, None)
    if located is not None:
        place = AgentStream.model_validate(located)
        stream = streams.get_handle(
            AgentEvents, id=place.handle.id, backend=place.handle.backend
        )
        return TurnStream(stream, place.topic, create_wait)
    first_run_id = description.raw_description.workflow_execution_info.first_run_id
    root = client.get_workflow_handle(workflow_id, first_execution_run_id=first_run_id or None)
    if description.status != WorkflowExecutionStatus.RUNNING:
        # A root that ended before its create never creates its Stream, so the read must not
        # wait for it. A Stream that does exist needs no wait, and its close or ``auto_close``
        # ends the read.
        create_wait = None
    return TurnStream(
        streams.workflow_stream(root, AgentEvents), TURN_EVENTS_TOPIC, create_wait
    )


async def next_turn_cursor(client: Client, workflow_id: str) -> str:
    """The cursor where ``workflow_id``'s Stream takes its next record.

    A client about to send a message reads this first, then follows the Stream from it, so it
    sees the turn it started without replaying the agent's history.
    """
    located = await turn_stream(client, workflow_id, wait=False)
    try:
        offset = await located.stream.get_offset()
    except StreamNotFoundError:
        # A root whose first Workflow Task has not completed has no Stream yet, so it starts at
        # the beginning.
        return ""
    return str(offset) if offset else ""


async def check_turn_cursor(client: Client, workflow_id: str, after: str) -> None:
    """Raise ``TurnCursorError`` now if ``after`` names no position on ``workflow_id``'s Stream.

    A caller that must refuse a bad cursor before it starts a response, such as an SSE route,
    calls this first. It reads at most one item and does not wait for new ones.
    """
    offset = parse_cursor(after)
    if offset is None:
        return
    located = await turn_stream(client, workflow_id, wait=False)
    items = located.stream.subscribe(
        located.turn_events(), from_offset=offset, wait_new_records=False
    )
    try:
        await anext(items, None)
    except (StreamOffsetError, StreamNotFoundError) as err:
        # A Stream that does not exist has no position past its beginning.
        raise TurnCursorError(str(err)) from err
    finally:
        await items.aclose()


def follow_turn_events(
    client: Client, workflow_id: str, *, after: str = "", subtree: bool = False
) -> AsyncGenerator[Record[AgentEvent], None]:
    """Yield ``workflow_id``'s turn events from the cursor ``after``, then tail live.

    See :func:`read_turn_events`. ``client`` carries the Stream plugin.
    """
    return _follow(client, workflow_id, after=after, subtree=subtree)


async def _follow(
    client: Client, workflow_id: str, *, after: str, subtree: bool
) -> AsyncGenerator[Record[AgentEvent], None]:
    parse_cursor(after)
    located = await turn_stream(client, workflow_id)
    records = read_turn_events(located, after=after, subtree=subtree)
    try:
        async for record in records:
            yield record
    finally:
        await records.aclose()


async def read_turn_events(
    located: TurnStream, *, after: str = "", subtree: bool = False
) -> AsyncGenerator[Record[AgentEvent], None]:
    """Yield an agent's turn events from the cursor ``after``, then tail live.

    With ``subtree``, the events of every subagent below the agent come too, in the one order
    of the shared Stream. Without it, only the agent's own Topic.

    Data records come out, and so does a marker for each ``Superseded`` item that withdraws a
    record already given: an activity that streams half an answer and then fails leaves those
    records in the Stream, and its retry writes different words, so a consumer that rendered
    the first half has to be told. Use ``cursor_after(record)`` to store a position for a later
    ``after``. A malformed cursor raises ``TurnCursorError`` at the first iteration. The read
    ends when the root agent's run chain closes the Stream.
    """
    offset = parse_cursor(after)
    items = located.stream.subscribe(
        located.turn_events(),
        prefixes=[subtree_prefix(located.topic)] if subtree else (),
        from_offset=offset,
        create_wait=located.create_wait,
    )
    # The newest record of each producer, to stamp a Superseded marker with.
    given: dict[str, Record[AgentEvent]] = {}
    try:
        async for item in items:
            if isinstance(item, Record):
                if not isinstance(item.data, AgentEvent):
                    # A Topic that a prefix matched is named at run time, so the read has no
                    # type for it and gives the plain JSON.
                    item = dataclasses.replace(item, data=AgentEvent.model_validate(item.data))
                if item.producer is not None:
                    given[item.producer.id] = item
                yield item
            elif isinstance(item, Superseded):
                marker = _superseded_marker(item, given)
                if marker is not None:
                    yield marker
            elif isinstance(item, Closed):
                return
    except StreamOffsetError as err:
        raise TurnCursorError(str(err)) from err
    except StreamNotFoundError:
        # A read with no create wait, of an agent that ended before it created its Stream,
        # has nothing to give.
        return
    finally:
        await items.aclose()


def _superseded_marker(
    item: Superseded, given: dict[str, Record[AgentEvent]]
) -> Record[AgentEvent] | None:
    """The marker for one ``Superseded`` item, stamped with the newest record it withdraws.

    The item names a producer and an attempt rather than a turn. Stamping it with the envelope
    of a withdrawn record is what makes it actionable: those are the records the failed attempt
    wrote. With no withdrawn record given yet, the caller holds none of them, so nothing comes
    out.
    """
    withdrawn = [record for record in given.values() if item.withdraws(record)]
    if not withdrawn:
        return None
    newest = max(withdrawn, key=lambda record: record.offset)
    previous = newest.data
    epoch = item.supersession.epoch
    old = newest.producer.epoch if newest.producer is not None else None
    return Record(
        topic=newest.topic,
        data=AgentEvent(
            agent_id=previous.agent_id,
            turn_id=previous.turn_id,
            turn_number=previous.turn_number,
            timestamp=previous.timestamp,
            event=AttemptSuperseded(
                producer_id=item.supersession.producer_id,
                superseded_attempt=(old.attempt or 0) if old is not None else 0,
                attempt=(epoch.attempt or 0) if epoch is not None else 0,
            ),
        ),
        offset=item.offset,
        producer=None,
    )
