# ABOUTME: The temporalio.contrib.streams backend of the agent event stream, the default of
# ``AgentWorkflowRunner``. An agent picks Workflow Streams with ``stream=WorkflowStream()``.
#
# An agent tree shares ONE Stream, which the root agent creates (``AgentEvents``). Each agent
# publishes on its own Topic (``turn_events`` for the root, ``subagent_topic(agent_id)`` below
# it), so one read of the Stream gives every agent's events in one order, and a Topic filter
# gives one agent's. Workflow code writes the Stream with Local Activities, an activity writes it
# through a ``BufferedPublisher``, and a client reads it through ``StreamClient``.
#
# Only this module imports ``temporalio.contrib.streams``, which no SDK release has yet. The rest
# of the harness imports it lazily, so an agent on ``WorkflowStream()`` runs on a released SDK.

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from pydantic import TypeAdapter
from temporalio import workflow
from temporalio.client import Client, WorkflowExecutionStatus

try:
    from temporalio.api.streamservice.v1 import ProducerKind
    from temporalio.contrib import streams
    from temporalio.contrib.streams import (
        ActivityStream,
        BufferedPublisher,
        ClientStream,
        Closed,
        Record,
        StreamClient,
        StreamConfigError,
        StreamNotFoundError,
        StreamOffsetError,
        StreamPlugin,
        Subscription,
        Superseded,
        WorkflowStream,
    )
    from temporalio.contrib.streams.workflow import create_stream_nowait
except ModuleNotFoundError as exc:
    raise RuntimeError(
        "The default stream= of AgentWorkflowRunner, ContribStream, needs a temporalio build "
        "with temporalio.contrib.streams, which no release has yet. Run "
        "`just install-streams-sdk`, or pass stream=WorkflowStream() to run on a release."
    ) from exc

from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AgentStream,
    AttemptSuperseded,
    subagent_topic,
    subtree_prefix,
)
from temporal_agent_harness.harness.stream_context import StreamBackend, TurnStreamContext

STORE_ENV = "STREAMS_STORE"
REDIS_URL_ENV = "STREAMS_REDIS_URL"
KEY_PREFIX_ENV = "STREAMS_KEY_PREFIX"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379"
# How long a read waits for a Stream that does not exist yet. A client learns the place from the
# agent's status or reply, which can come before the create's Local Activity is done.
_WAIT_FOR_CREATE = timedelta(minutes=1)
# How long a read waits with no item before it asks whether the agent still runs. A terminated
# or timed-out run closes nothing on Redis or memory, so the read has to find out.
_LIVENESS_CHECK = timedelta(seconds=5)
# Every client of this process shares one memory plugin, so they all see the same Streams.
_memory: StreamPlugin | None = None
# Said when a worker has no Stream plugin, which fails the Workflow Task until one has.
_NO_PLUGIN = (
    "The default stream= of AgentWorkflowRunner, ContribStream, needs a Stream plugin on the "
    "worker's client, such as Client.connect(..., plugins=[AgentHarnessPlugin(), "
    "streams_from_env()]) with STREAMS_STORE set. stream=WorkflowStream() needs none."
)


@streams.defn(name="agent_events")
class AgentEvents:
    """The Stream an agent tree publishes on. The root agent creates it, and its subagents
    write it too.

    Its one declared Topic is the root's own. A subagent's Topic (``subagent_topic``) is made
    at run time, so a handle opens it with ``type=AgentEvent``.
    """

    turn_events = streams.topic(AgentEvent, name=TURN_EVENTS_TOPIC)


class TurnCursorError(ValueError):
    """A turn cursor that is not a position on the agent's Stream."""


def streams_from_env() -> StreamPlugin | None:
    """The Stream plugin for the store in ``STREAMS_STORE``, or ``None`` when it is unset.

    ``redis`` is the Redis at ``STREAMS_REDIS_URL``. The web app, the Nexus adapter and the
    workers are separate processes, and they must all reach one Redis. ``STREAMS_KEY_PREFIX``
    puts the keys under a prefix of their own, which keeps two deployments (or two test runs)
    on one Redis apart. ``memory`` keeps the Streams in this process, for tests and a single
    process only. Unset means the process serves agents on ``WorkflowStream()`` only.

    Register the result once, as ``Client.connect(..., plugins=[streams])``. Workers built from
    that client take it. One plugin serves one client connection, so a second connection needs
    a plugin of its own, except the memory plugin, which all of them share.
    """
    global _memory
    store = os.environ.get(STORE_ENV)
    if not store:
        return None
    if store == "memory":
        from temporalio.contrib.streams.testing import MemoryStreamPlugin

        if _memory is None:
            _memory = MemoryStreamPlugin()
        return _memory
    if store != "redis":
        raise ValueError(f"{STORE_ENV}={store!r} is not a Stream store")
    from temporalio.contrib.streams.redis import RedisStreamPlugin

    return RedisStreamPlugin(
        os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL),
        key_prefix=os.environ.get(KEY_PREFIX_ENV) or None,
    )


class ContribStream(StreamBackend):
    """Selects ``temporalio.contrib.streams`` for an agent tree's turn events.

    ``AgentWorkflowRunner`` uses ``ContribStream()`` when an agent passes no ``stream=``. Pass
    one of your own for another backend or retention. The root agent creates the tree's
    Stream, and its subagents publish there too, whatever their own ``stream=`` says. The
    worker's client needs a Stream plugin, such as the one ``streams_from_env`` gives.

    Args:
        backend: The name of the Stream plugin's backend. ``None`` is the worker's.
        retention: How long the records stay after the Stream closes. ``None`` is the
            backend's default.
    """

    def __init__(self, *, backend: str | None = None, retention: timedelta | None = None) -> None:
        self.backend = backend
        self.retention = retention

    def open_sink(self) -> ContribSink:
        return open_root(self)


# ---------------------------------------------------------------------------
# Workflow side
# ---------------------------------------------------------------------------


class ContribSink:
    """What the runner publishes through on ``temporalio.contrib.streams``.

    Each write is a Local Activity. Its future gives the record's offset, which tells the
    runner where its own records reached, and its error, which the runner logs. A store that
    refuses a write costs the live view that event, and the agent goes on.
    """

    def __init__(self, stream: WorkflowStream[Any], place: AgentStream) -> None:
        self._stream = stream
        self.place = place
        # The offset past the newest record this run's writes reached.
        self._next_offset = 0

    @property
    def topic(self) -> str:
        return self.place.topic

    def publish(self, event: AgentEvent) -> None:
        self._write(self.place.topic, event)

    def announce(self, event: AgentEvent, topic: str) -> None:
        """Publish ``event`` on another agent's Topic of the same Stream."""
        self._write(topic, event)

    def accepted_offset(self) -> int:
        return 0

    def accepted_cursor(self) -> str:
        return str(self._next_offset) if self._next_offset else ""

    def child_place(self, agent_id: str) -> AgentStream:
        return AgentStream(handle=self.place.handle, topic=subagent_topic(agent_id))

    def activity_stream(self) -> ActivityStream[Any]:
        return ActivityStream.from_handle(self._stream)

    def _write(self, topic: str, event: AgentEvent) -> None:
        # ``publish_nowait`` drops the write's future, and the runner needs its offset and
        # its error. ``_append`` is the call that ``publish_nowait`` makes.
        try:
            written = self._stream._append(topic, (event,))
        except StreamConfigError as err:
            raise StreamConfigError(f"{_NO_PLUGIN} ({err})") from err
        written.add_done_callback(self._written)

    def _written(self, written: asyncio.Future[int]) -> None:
        if written.cancelled():
            return
        error = written.exception()
        if error is not None:
            workflow.logger.warning("An agent event was not written to its Stream: %s", error)
            return
        self._next_offset = max(self._next_offset, written.result() + 1)


def open_root(choice: ContribStream) -> ContribSink:
    """Create the tree's Stream in a root agent's ``@agent.init``.

    The Stream closes when the run chain closes. It has no ``auto_close``, so an idle session
    keeps its Stream. A reader of a run that ended with no close finds that out itself.
    """
    try:
        stream = create_stream_nowait(
            AgentEvents, retention=choice.retention, backend=choice.backend
        )
    except StreamConfigError as err:
        raise StreamConfigError(f"{_NO_PLUGIN} ({err})") from err
    place = AgentStream(handle=_handle_json(stream), topic=TURN_EVENTS_TOPIC)
    return ContribSink(stream, place)


def open_place(place: AgentStream) -> ContribSink:
    """Write the Stream at ``place``, for a subagent or for a run after continue-as-new."""
    stream = TypeAdapter(WorkflowStream[AgentEvents]).validate_python(place.handle)
    return ContribSink(stream, place)


def _handle_json(stream: Any) -> dict[str, str]:
    return TypeAdapter(WorkflowStream[AgentEvents]).dump_python(stream, mode="json")


# ---------------------------------------------------------------------------
# Activity side
# ---------------------------------------------------------------------------


@asynccontextmanager
async def activity_publisher(
    context: TurnStreamContext, *, max_delay: timedelta
) -> AsyncIterator[BufferedPublisher[AgentEvent]]:
    """Buffer an activity's events for the Stream and Topic in ``context``.

    Leaving the block writes the tail, also when the activity fails, so a reader sees what
    the attempt produced up to the failure.
    """
    assert context.stream is not None
    topic = context.stream.topic(context.topic, type=AgentEvent)
    async with BufferedPublisher(topic, max_delay=max_delay) as events:
        yield events


# ---------------------------------------------------------------------------
# Client side
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TurnItem:
    """One event a read gives, with where it is on the Stream."""

    event: AgentEvent
    # The record's own offset, in decimal. A synthesized marker has the offset of its
    # ``Superseded`` item.
    offset: str
    # The cursor that resumes just past this item.
    cursor: str
    # Whether the agent that the read is for published it, and not a subagent below it.
    own: bool


def parse_cursor(cursor: str) -> int | None:
    """The offset in a cursor, or ``None`` for the beginning.

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


def _client_stream(client: Client, place: AgentStream) -> ClientStream[AgentEvents]:
    handle = place.handle
    return StreamClient(client).get_handle(
        AgentEvents, id=handle["id"], backend=handle.get("backend") or None
    )


async def next_offset(client: Client, place: AgentStream) -> int:
    """The offset where the agent's Stream takes its next record, or 0 before its create."""
    try:
        return await _client_stream(client, place).get_offset()
    except StreamNotFoundError:
        return 0


async def check_turn_cursor(client: Client, place: AgentStream, after: str) -> None:
    """Raise ``TurnCursorError`` now if ``after`` is not a position on the agent's Stream.

    A caller that must refuse a bad cursor before it starts a response, such as an SSE route,
    calls this first. It reads at most one item and does not wait for new ones.
    """
    offset = parse_cursor(after)
    if offset is None:
        return
    stream = _client_stream(client, place)
    items = stream.subscribe(
        stream.topic(place.topic, type=AgentEvent), from_offset=offset, wait_new_records=False
    )
    try:
        await anext(items, None)
    except (StreamOffsetError, StreamNotFoundError) as err:
        # A Stream that does not exist has no position past its beginning.
        raise TurnCursorError(str(err)) from err
    finally:
        await items.aclose()


async def read_turn_events(
    client: Client,
    place: AgentStream,
    *,
    workflow_id: str,
    after: str = "",
    subtree: bool = False,
) -> AsyncGenerator[TurnItem, None]:
    """Yield an agent's turn events from the cursor ``after``, then tail live.

    ``place`` is the agent's, from its status or a send's reply. With ``subtree``, the events
    of every subagent below the agent come too, in the one order of the shared Stream.

    A ``Superseded`` item that withdraws records this read gave comes out as an
    ``AttemptSuperseded`` event with their offsets. The read ends at the Stream's close, and
    when ``workflow_id`` or the Stream's owner no longer runs, since a terminated run closes
    nothing on Redis or memory. A malformed cursor raises ``TurnCursorError``.
    """
    offset = parse_cursor(after)
    stream = _client_stream(client, place)
    items: Subscription[AgentEvent] = stream.subscribe(
        stream.topic(place.topic, type=AgentEvent),
        prefixes=[subtree_prefix(place.topic)] if subtree else (),
        from_offset=offset,
        create_wait=_WAIT_FOR_CREATE,
    )
    # The newest record of each run and each activity that wrote a record this read gave, to
    # stamp a marker with. A Superseded item withdraws one run's or one activity's records, so
    # one agent's. One entry per run and per activity, not per record.
    writers: dict[str, tuple[AgentEvent, bool]] = {}
    owner = (place.handle.get("workflowId"), place.handle.get("firstExecutionRunId"))
    pending: asyncio.Future[Any] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(items.__anext__())
            done, _ = await asyncio.wait({pending}, timeout=_LIVENESS_CHECK.total_seconds())
            if not done:
                if await _ended(client, workflow_id, owner):
                    return
                continue
            ready, pending = pending, None
            try:
                item = ready.result()
            except StopAsyncIteration:
                return
            cursor = str(items.next_offset or "")
            if isinstance(item, Record):
                data = item.data
                if not isinstance(data, AgentEvent):
                    # A Topic that a prefix matched is made at run time, so the read has no
                    # type for it and gives the plain JSON.
                    data = AgentEvent.model_validate(data)
                own = item.topic == place.topic
                producer = item.producer
                if producer is not None:
                    if producer.run_id:
                        writers[producer.run_id] = (data, own)
                    if producer.kind == ProducerKind.PRODUCER_KIND_ACTIVITY:
                        writers[producer.id] = (data, own)
                yield TurnItem(event=data, offset=str(item.offset), cursor=cursor, own=own)
            elif isinstance(item, Superseded):
                marked = _superseded_marker(item, writers)
                if marked is not None:
                    marker, own = marked
                    yield TurnItem(event=marker, offset=str(item.offset), cursor=cursor, own=own)
            elif isinstance(item, Closed):
                return
    except StreamOffsetError as err:
        raise TurnCursorError(str(err)) from err
    except StreamNotFoundError:
        # An agent that ended before its create has nothing to give.
        return
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await items.aclose()


def _superseded_marker(
    item: Superseded, writers: dict[str, tuple[AgentEvent, bool]]
) -> tuple[AgentEvent, bool] | None:
    """The marker for one ``Superseded`` item and whether it is the read agent's.

    ``None`` when it withdraws nothing this read gave, such as a Workflow Task retry that
    wrote no record.
    """
    if not item.withdrawn_offsets:
        return None
    supersession = item.supersession
    found = next(
        (
            writers[key]
            for key in (
                supersession.producer_id,
                supersession.run_id or "",
                supersession.withdraw_run or "",
            )
            if key in writers
        ),
        None,
    )
    if found is None:
        return None
    writer, own = found
    marker = AgentEvent(
        agent_id=writer.agent_id,
        turn_id=writer.turn_id,
        turn_number=writer.turn_number,
        timestamp=writer.timestamp,
        event=AttemptSuperseded(withdrawn_offsets=[str(o) for o in item.withdrawn_offsets]),
    )
    return marker, own


async def _ended(client: Client, workflow_id: str, owner: tuple[str | None, str | None]) -> bool:
    """Whether the read's agent or the Stream's owner chain no longer runs."""
    for wid, first_run_id in ((workflow_id, None), owner):
        if not wid:
            continue
        try:
            description = await client.get_workflow_handle(wid).describe()
        except Exception:  # noqa: BLE001, since a workflow that is gone writes nothing more
            return True
        info = description.raw_description.workflow_execution_info
        if first_run_id and info.first_run_id and info.first_run_id != first_run_id:
            return True
        if description.status != WorkflowExecutionStatus.RUNNING:
            return True
    return False

