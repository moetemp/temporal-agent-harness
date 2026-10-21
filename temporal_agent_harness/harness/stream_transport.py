# ABOUTME: How the harness reaches its Streams: the one place the Stream plugin is built, the
# publisher an activity uses, and the readers every client-side consumer of turn events shares.
# The plugin is registered once on the client. Workflow code writes through ``workflow_stream``,
# activities through ``activity_stream``, and clients read through ``for_handle``.
#
# An agent tree shares ONE Stream: the root agent's default Stream. Each agent publishes on its
# own Topic (``turn_events`` for the root, ``subagent_topic(agent_id)`` below it), so one read of
# the Stream gives every agent's events in one order, and a Topic filter gives one agent's.

from __future__ import annotations

import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHandle
from temporalio.contrib.streams import (
    ClientStream,
    Closed,
    Idle,
    Record,
    StreamNotFoundError,
    StreamOffsetError,
    StreamRef,
    Superseded,
    Topic,
    activity_stream,
    for_handle,
)
from temporalio.contrib.streams.redis import RedisStreamPlugin

from temporal_agent_harness.harness.agent_protocol import (
    AGENT_STREAM_MEMO,
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AgentStream,
    AttemptSuperseded,
    in_subtree,
)

REDIS_URL_ENV = "STREAMS_REDIS_URL"
KEY_PREFIX_ENV = "STREAMS_KEY_PREFIX"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379"
# How long one read waits for a Stream that no agent has written yet, before it looks at the
# root's run again. A fresh session has no Stream until its first event, and a client may start
# reading before that.
_CREATE_WINDOW = timedelta(seconds=5)


class TurnCursorError(ValueError):
    """A turn cursor that is not a position on the agent's Stream."""


def streams_from_env() -> RedisStreamPlugin:
    """The Redis Stream plugin at ``STREAMS_REDIS_URL``.

    The web app, the Nexus adapter and the workers are separate processes, and they must all
    reach one Redis. ``STREAMS_KEY_PREFIX`` puts the keys under a prefix of their own, which
    keeps two deployments (or two test runs) on one Redis apart.

    Register the result once, as ``Client.connect(..., plugins=[streams])``. Workers built from
    that client take it, workflow code and activities reach it through the facade, and any other
    code through ``for_handle(handle)``. One plugin serves one client connection, so a second
    connection needs a plugin of its own.
    """
    return RedisStreamPlugin(
        os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL),
        key_prefix=os.environ.get(KEY_PREFIX_ENV) or None,
    )


def parse_cursor(cursor: str) -> int | None:
    """The offset a stored cursor names, or None for the beginning.

    A cursor is the offset to read from next, in decimal. Offsets are positive, so zero is the
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
    """The Stream an agent publishes on, the Topic that is its own, and the root's handle."""

    stream: ClientStream
    topic: str
    root: WorkflowHandle[Any, Any]

    def turn_events(self) -> Topic[AgentEvent]:
        """The agent's own Topic, typed."""
        return self.stream.topic(self.topic, type=AgentEvent)


async def turn_stream(client: Client, workflow_id: str, *, wait: bool = True) -> TurnStream:
    """The Stream and Topic of the agent ``workflow_id``. ``client`` carries the Stream plugin.

    A top-level agent publishes on its own default Stream. A subagent publishes on its root's,
    and its memo says where, so a client that knows only the subagent finds the Stream with one
    Describe call. With ``wait``, a read of a Stream that does not exist yet waits for it, and
    ends when the root's run ends.
    """
    description = await client.get_workflow_handle(workflow_id).describe()
    located = await description.memo_value(AGENT_STREAM_MEMO, None)
    if located is not None:
        place = AgentStream.model_validate(located)
        root = client.get_workflow_handle(place.root_workflow_id)
        return TurnStream(_open(root, wait), place.topic, root)
    first_run_id = description.raw_description.workflow_execution_info.first_run_id
    root = client.get_workflow_handle(workflow_id, first_execution_run_id=first_run_id or None)
    return TurnStream(_open(root, wait), TURN_EVENTS_TOPIC, root)


def _open(handle: WorkflowHandle[Any, Any], wait: bool) -> ClientStream:
    if not wait:
        return for_handle(handle)
    # The read ends with the root's run: its close ends it, and so does a run that ended with
    # no close, such as a terminated one.
    return for_handle(handle, wait_for_create=_CREATE_WINDOW, end_with_run=True)


async def _run_ended(handle: WorkflowHandle[Any, Any]) -> bool:
    description = await handle.describe()
    return (
        description.status is not None
        and description.status != WorkflowExecutionStatus.RUNNING
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
        # A session that published nothing yet has no Stream, so it starts at the beginning.
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
        topics=[located.turn_events()], from_offset=offset, wait_new_records=False
    )
    try:
        await anext(items, None)
    except (StreamOffsetError, StreamNotFoundError) as err:
        # A Stream that does not exist has no position past its beginning.
        raise TurnCursorError(str(err)) from err
    finally:
        await _close(items)


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
    ends when the root agent's run closes.
    """
    offset = parse_cursor(after)
    own = located.turn_events()
    # The newest record of each producer, to stamp a Superseded marker with.
    given: dict[str, Record[AgentEvent]] = {}
    started = False
    while True:
        items = located.stream.subscribe(topics=None if subtree else [own], from_offset=offset)
        try:
            async for item in items:
                started = True
                if isinstance(item, Record):
                    if subtree and not in_subtree(item.topic, located.topic):
                        continue
                    record = _typed(item)
                    if record.producer is not None:
                        given[record.producer.id] = record
                    yield record
                elif isinstance(item, Superseded):
                    marker = _superseded_marker(item, given)
                    if marker is not None:
                        yield marker
                elif isinstance(item, (Closed, Idle)):
                    return
            return
        except StreamNotFoundError:
            # The read's wait for the Stream to exist does not end with the run, so a session
            # that ended before its first event would hold the reader forever.
            if started:
                raise
            if await _run_ended(located.root):
                return
        except StreamOffsetError as err:
            raise TurnCursorError(str(err)) from err
        finally:
            await _close(items)


async def _close(items: AsyncIterator[Any]) -> None:
    # ``subscribe`` gives an async generator, typed as an iterator, so stop the read explicitly.
    close = getattr(items, "aclose", None)
    if close is not None:
        await close()


def _typed(record: Record[Any]) -> Record[AgentEvent]:
    # A read of every Topic decodes the Topics it has no type for to plain JSON, and a
    # subagent's Topic is named at run time.
    if isinstance(record.data, AgentEvent):
        return record
    return Record(
        topic=record.topic,
        data=AgentEvent.model_validate(record.data),
        offset=record.offset,
        producer=record.producer,
    )


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


class ActivityPublisher:
    """Publishes values onto one Topic from an activity.

    ``publish`` is synchronous, so an SDK's streaming callback can call it. The Topic buffers
    the records and flushes every ``batch_interval``, and leaving the context flushes the tail.
    """

    def __init__(self, topic: Topic[Any]) -> None:
        self._topic = topic

    def publish(self, value: Any) -> None:
        self._topic.publish(value)


@asynccontextmanager
async def publisher_for_activity(
    topic: str,
    *,
    stream_id: str | None = None,
    batch_interval: timedelta = timedelta(milliseconds=50),
) -> AsyncIterator[ActivityPublisher]:
    """A publisher onto ``topic`` of the Stream ``stream_id``, from an activity.

    With no ``stream_id``, the default Stream of the workflow that scheduled this activity. The
    records carry the activity's own Producer id and attempt, so a retry withdraws what the
    failed attempt wrote, and readers get a ``Superseded`` item for it.
    """
    stream = activity_stream(
        StreamRef(stream_id) if stream_id else None, batch_interval=batch_interval
    )
    # The type only decodes a read, and this Topic is only written.
    publisher = ActivityPublisher(stream.topic(topic, type=object))
    try:
        yield publisher
    finally:
        # The tail goes out even when the activity is failing, so a reader sees what was
        # produced up to the failure.
        await stream.flush()
