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
from temporalio.service import RPCError, RPCStatusCode

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
    """Raise ``TurnCursorError`` at once if ``after`` is not a position on the agent's Stream.

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

    A ``Superseded`` item that withdraws records this read or an earlier one gave comes out
    as an ``AttemptSuperseded`` event with their offsets. A read that starts at such a
    marker's cursor gives the marker again, with the same offsets. The read ends at the
    Stream's close, and when ``workflow_id`` or the Stream's owner no longer runs, since a
    terminated run closes nothing on Redis or memory. A malformed cursor raises
    ``TurnCursorError``.
    """
    reading = _Read(client, place, after=after, subtree=subtree)
    owner = (place.handle.get("workflowId"), place.handle.get("firstExecutionRunId"))
    items = reading.subscribe(reading.start, create_wait=_WAIT_FOR_CREATE)
    pending: asyncio.Future[Any] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(items.__anext__())
            done, _ = await asyncio.wait({pending}, timeout=_LIVENESS_CHECK.total_seconds())
            if not done and not await _ended(client, workflow_id, owner):
                continue
            ready, pending = pending, None
            if not ready.done():
                # The records that the run wrote just before it ended can still be on their
                # way, so the read goes on to the newest record and no further.
                ready.cancel()
                await asyncio.gather(ready, return_exceptions=True)
                await items.aclose()
                items = reading.subscribe(items.next_offset, wait_new_records=False)
                continue
            try:
                record = ready.result()
            except StopAsyncIteration:
                return
            if isinstance(record, Closed):
                return
            item = await reading.take(record, items.next_offset)
            if item is not None:
                yield item
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


class _Read:
    """One read of an agent's Topic, and what it gave, so a marker can tell what it withdraws."""

    def __init__(self, client: Client, place: AgentStream, *, after: str, subtree: bool) -> None:
        self.start = parse_cursor(after)
        self._place = place
        self._stream = _client_stream(client, place)
        self._topic = self._stream.topic(place.topic, type=AgentEvent)
        self._prefixes = [subtree_prefix(place.topic)] if subtree else []
        self._shown: dict[str, _Shown] = {}
        # What earlier reads gave before ``start``, read at the first ``Superseded`` item.
        self._before: dict[str, _Shown] | None = None

    def subscribe(self, start: int | None, **options: Any) -> Subscription[AgentEvent]:
        return self._stream.subscribe(
            self._topic, prefixes=self._prefixes, from_offset=start, **options
        )

    async def take(self, item: Any, next_offset: int | None) -> TurnItem | None:
        """The ``TurnItem`` for one item of the read, or ``None`` for an item to skip."""
        cursor = str(next_offset or "")
        if isinstance(item, Record):
            shown = self._note(self._shown, item)
            return TurnItem(
                event=shown.event, offset=str(item.offset), cursor=cursor, own=shown.own
            )
        if isinstance(item, Superseded):
            marked = await self._marker(item)
            if marked is not None:
                marker, own = marked
                return TurnItem(event=marker, offset=str(item.offset), cursor=cursor, own=own)
        return None

    def _note(self, shown: dict[str, _Shown], record: Record[Any]) -> _Shown:
        entry = _Shown(record, _event(record), record.topic == self._place.topic, [])
        if record.producer is not None:
            key = repr(record.producer)
            if key in shown:
                entry.offsets = shown[key].offsets
            entry.offsets.append(record.offset)
            shown[key] = entry
        return entry

    async def _marker(self, item: Superseded) -> tuple[AgentEvent, bool] | None:
        """The marker for one ``Superseded`` item and whether it is the read agent's.

        ``None`` when it withdraws nothing that a reader of this Topic was given, such as a
        Workflow Task retry that wrote no record.
        """
        offsets, newest = _withdraw(item, self._shown)
        if self.start is not None:
            # A reader of this Topic got the records before ``start`` from an earlier read.
            if self._before is None:
                self._before = await self._read_before(self.start)
            earlier, writer = _withdraw(item, self._before)
            offsets += earlier
            newest = newest or writer
        if not offsets or newest is None:
            return None
        writer = newest.event
        marker = AgentEvent(
            agent_id=writer.agent_id,
            turn_id=writer.turn_id,
            turn_number=writer.turn_number,
            timestamp=writer.timestamp,
            event=AttemptSuperseded(withdrawn_offsets=[str(o) for o in sorted(offsets)]),
        )
        return marker, newest.own

    async def _read_before(self, start: int) -> dict[str, _Shown]:
        """The records before ``start`` that no ``Superseded`` item withdrew there.

        It reads the Topic again, which costs a read only when an attempt superseded another.
        """
        shown: dict[str, _Shown] = {}
        earlier = self.subscribe(None, wait_new_records=False)
        try:
            async for old in earlier:
                if old.offset >= start:
                    break
                if isinstance(old, Record):
                    self._note(shown, old)
                elif isinstance(old, Superseded):
                    _withdraw(old, shown)
        finally:
            await earlier.aclose()
        return shown


@dataclass
class _Shown:
    """The records of one Producer that a read gave. ``Superseded.withdraws`` looks at the
    Producer only, so they are withdrawn together."""

    # The newest of them, with its event and whether it is the read agent's own.
    record: Record[Any]
    event: AgentEvent
    own: bool
    offsets: list[int]


def _withdraw(item: Superseded, shown: dict[str, _Shown]) -> tuple[list[int], _Shown | None]:
    """Drop from ``shown`` what ``item`` withdraws, and give its offsets and newest record."""
    offsets: list[int] = []
    newest: _Shown | None = None
    for key, entry in list(shown.items()):
        if item.withdraws(entry.record):
            # A withdrawn record stays withdrawn, so a later item does not withdraw it again.
            del shown[key]
            offsets += entry.offsets
            if newest is None or entry.record.offset > newest.record.offset:
                newest = entry
    return offsets, newest


def _event(record: Record[Any]) -> AgentEvent:
    data = record.data
    if not isinstance(data, AgentEvent):
        # A Topic that a prefix matched is made at run time, so the read has no type for it
        # and gives the plain JSON.
        data = AgentEvent.model_validate(data)
    return data


async def _ended(client: Client, workflow_id: str, owner: tuple[str | None, str | None]) -> bool:
    """Whether the read's agent or the Stream's owner chain no longer runs."""
    for wid, first_run_id in ((workflow_id, None), owner):
        if not wid:
            continue
        try:
            description = await client.get_workflow_handle(wid).describe()
        except RPCError as err:
            # A workflow that is gone writes nothing more. Another error tells nothing about it.
            if err.status == RPCStatusCode.NOT_FOUND:
                return True
            raise
        info = description.raw_description.workflow_execution_info
        if first_run_id and info.first_run_id and info.first_run_id != first_run_id:
            return True
        if description.status != WorkflowExecutionStatus.RUNNING:
            return True
    return False
