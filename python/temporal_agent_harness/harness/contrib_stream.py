# ABOUTME: The temporalio.common Streams backend of the agent event stream, the default of
# ``AgentWorkflowRunner``. An agent picks Workflow Streams with ``stream=WorkflowStream()``.
#
# An agent tree shares ONE Stream, which the root agent creates (``AgentEvents``). Each agent
# publishes on its own Topic (``turn_events`` for the root, ``subagent_topic(agent_id)`` below
# it), so one read of the Stream gives every agent's events in one order, and a Topic filter
# gives one agent's. Workflow code writes the Stream with Local Activities, an activity writes it
# through a ``BufferedPublisher``, and a client reads it through ``Client.stream``.
#
# Only this module imports the Streams API of ``temporalio.common``, which no SDK release has yet.
# The rest of the harness imports it lazily, so an agent on ``WorkflowStream()`` runs on a release.

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from pydantic import TypeAdapter
from temporalio import common, workflow
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.common import RawValue
from temporalio.service import RPCError, RPCStatusCode

# A release's ``temporalio.common`` has no Stream names, so their import raises ``ImportError``.
try:
    from temporalio.common import (
        ActivityStream,
        BufferedPublisher,
        ClientStream,
        Closed,
        Record,
        StreamConfigError,
        StreamNotFoundError,
        StreamOffsetError,
        Subscription,
        Superseded,
        WorkflowStream,
    )
    from temporalio.contrib.stream.workflow import create_stream_nowait
except ImportError as exc:
    raise RuntimeError(
        "The default stream= of AgentWorkflowRunner, ContribStream, needs a temporalio build "
        "with the Streams API in temporalio.common, which no release has yet. Run "
        "`just install-streams-sdk`, or pass stream=WorkflowStream() to run on a release."
    ) from exc

from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AgentEventType,
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
# How long a read waits with no item before it asks whether the agent still runs. The end of a
# run closes nothing on Redis or memory, so the read has to find out.
_LIVENESS_CHECK = timedelta(seconds=5)
# The errors of that check that can go away on their own.
_TRANSIENT = frozenset(
    {
        RPCStatusCode.UNAVAILABLE,
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.RESOURCE_EXHAUSTED,
        RPCStatusCode.ABORTED,
    }
)
# Every client of this process shares one memory backend, so they all see the same Streams.
_memory: common.StreamBackend | None = None
# Said when a worker has no Stream backend, which fails the Workflow Task until one has.
_NO_BACKEND = (
    "The default stream= of AgentWorkflowRunner, ContribStream, needs a Stream backend on the "
    "worker's client, such as Client.connect(..., plugins=[AgentHarnessPlugin()], "
    "stream_backends=stream_backends_from_env()) with STREAMS_STORE set. "
    "stream=WorkflowStream() needs none."
)


_STREAM_NAME = "agent_events"


@common.defn(name=_STREAM_NAME)
class AgentEvents:
    """The Stream an agent tree publishes on. The root agent creates it, and its subagents
    write it too.

    Its one declared Topic is the root's own. A subagent's Topic (``subagent_topic``) is made
    at run time, so a handle opens it with ``type=AgentEvent``.
    """

    turn_events = common.topic(AgentEvent, name=TURN_EVENTS_TOPIC)


class TurnCursorError(ValueError):
    """A turn cursor that is not a position on the agent's Stream."""


def stream_backends_from_env() -> list[common.StreamBackend]:
    """The Stream backend that ``STREAMS_STORE`` names, in a list, or none when it is unset.

    ``redis`` is the Redis at ``STREAMS_REDIS_URL``. The web app, the Nexus adapter and the
    workers are separate processes, and they must all reach one Redis. ``STREAMS_KEY_PREFIX``
    puts the keys under a prefix of their own, which keeps two deployments (or two test runs)
    on one Redis apart. ``memory`` keeps the Streams in this process, for tests and a single
    process only. Unset means the process serves agents on ``WorkflowStream()`` only.

    Register the result as ``Client.connect(..., stream_backends=backends)``. Workers built from
    that client take it. The backend has ``default=True``, since a place found without asking
    the agent (``root_place``) is on the client's default backend. Every call gives the one
    memory backend, so all clients of the process see the same Streams.
    """
    global _memory
    store = os.environ.get(STORE_ENV)
    if not store:
        return []
    if store == "memory":
        from temporalio.contrib.stream.testing import MemoryStreamBackend

        if _memory is None:
            _memory = MemoryStreamBackend(default=True)
        return [_memory]
    if store != "redis":
        raise ValueError(f"{STORE_ENV}={store!r} is not a Stream store")
    from temporalio.contrib.stream import RedisStreamBackend

    return [
        RedisStreamBackend(
            os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL),
            key_prefix=os.environ.get(KEY_PREFIX_ENV) or None,
            default=True,
        )
    ]


class ContribStream(StreamBackend):
    """Selects the Streams API of ``temporalio.common`` for an agent tree's turn events.

    ``AgentWorkflowRunner`` uses ``ContribStream()`` when an agent passes no ``stream=``. Pass
    one of your own for another backend or retention. The root agent creates the tree's
    Stream, and its subagents publish there too, whatever their own ``stream=`` says. The
    worker's client needs a Redis or memory Stream backend, such as the one
    ``stream_backends_from_env`` gives.

    Args:
        backend: The name of the Stream backend. ``None`` is the worker's.
        retention: The age after which a record is dropped. Each write drops the records
            older than it, also while the Stream is open, so a long session loses its first
            events. A closed Stream stays one retention after its close. ``None`` is the
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
    """What the runner publishes through on a ``temporalio.common`` Stream.

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
        # The runner needs each write's offset and goes on past a refused write. No public call
        # gives the write's future, and ``publish_nowait`` fails the Workflow Task on an error.
        try:
            written = self._stream._append(topic, (event,))
        except StreamConfigError as err:
            raise StreamConfigError(f"{_NO_BACKEND} ({err})") from err
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

    Nothing closes the Stream. On Redis and memory the end of the run chain closes nothing, and
    the harness never closes it, so an idle session keeps its Stream. An open Stream goes after
    the backend's ``safety_ttl`` with no write. A reader of a run that ended finds that out.
    """
    try:
        stream = create_stream_nowait(
            AgentEvents, retention=choice.retention, backend=choice.backend
        )
    except StreamConfigError as err:
        raise StreamConfigError(f"{_NO_BACKEND} ({err})") from err
    place = AgentStream(handle=_handle_json(stream), topic=TURN_EVENTS_TOPIC)
    return ContribSink(stream, place)


def open_place(place: AgentStream) -> ContribSink:
    """Write the Stream at ``place``, for a subagent or for a run after continue-as-new."""
    stream = TypeAdapter(WorkflowStream[AgentEvents]).validate_python(place.handle)
    return ContribSink(stream, place)


def root_place(client: Client, workflow_id: str, first_run_id: str) -> AgentStream:
    """The place of the root agent ``workflow_id``, found without asking the agent.

    The id of a Stream that a run chain creates comes from its workflow id and its first run,
    so the handle needs no call. Only the agent's status gives its backend, so this is the
    client's default one.
    """
    handle = client.get_workflow_handle(workflow_id, first_execution_run_id=first_run_id)
    return AgentStream(handle=_handle_json(handle.stream(AgentEvents)), topic=TURN_EVENTS_TOPIC)


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
    # The record's own offset, in decimal. A marker has none, since its ``Superseded`` item
    # shares an offset with the first record of the next attempt, and a client tells events
    # apart by their offsets.
    offset: str
    # The cursor that resumes just past this item.
    cursor: str
    # Whether the agent that the read is for published it, and not a subagent below it.
    own: bool


def parse_cursor(cursor: str) -> int | None:
    """The offset in a cursor, or ``None`` for the beginning.

    A cursor is the offset to read from next, in decimal. Offsets are above zero, so zero is the
    beginning too. A cursor that a read gives can say more after the offset (``_Position``).
    """
    return _position(cursor).offset


# ``<offset>``, then ``.s`` when the ``Superseded`` item at the offset was given, then
# ``.t<offset>`` with where the oldest open turn started.
_CURSOR = re.compile(r"(\d+)(\.s)?(?:\.t(\d+))?", re.ASCII)


@dataclass(frozen=True)
class _Position:
    """Where a read resumes, and what it needs to know of the reads before it."""

    offset: int | None
    # A ``Superseded`` item and the first record of its batch share an offset, so a read that
    # resumes past the item skips it at ``offset``.
    past_superseded: bool = False
    # The first offset of the oldest turn still open. A retry withdraws records of its own
    # turn only, so a later read looks back no further.
    turn_start: int | None = None

    def encode(self) -> str:
        if self.offset is None:
            return ""
        cursor = str(self.offset)
        if self.past_superseded:
            cursor += ".s"
        if self.turn_start is not None:
            cursor += f".t{self.turn_start}"
        return cursor


def _position(cursor: str) -> _Position:
    if not cursor:
        return _Position(None)
    parsed = _CURSOR.fullmatch(cursor)
    if parsed is None:
        raise TurnCursorError(f"not a turn cursor: {cursor!r}")
    offset = int(parsed[1])
    turn_start = int(parsed[3]) if parsed[3] is not None else None
    if turn_start is not None and turn_start > offset:
        raise TurnCursorError(f"not a turn cursor: {cursor!r}")
    return _Position(offset or None, parsed[2] is not None, turn_start)


def _client_stream(client: Client, place: AgentStream) -> ClientStream[AgentEvents]:
    handle = place.handle
    return client.stream(AgentEvents, id=handle["id"], backend=handle.get("backend") or None)


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
        stream.topic(place.topic, type=AgentEvent), offset=offset, wait_new_records=False
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

    A ``Superseded`` item that withdraws records this read gave, or that an earlier read gave
    in the same turn, comes out as an ``AttemptSuperseded`` event with their offsets. The read
    ends at the Stream's close, and when ``workflow_id`` or the Stream's owner no longer runs,
    since the end of a run closes nothing on Redis or memory. A malformed cursor raises
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
        position = _position(after)
        self.start = position.offset
        self._place = place
        self._stream = _client_stream(client, place)
        self._topic = self._stream.topic(place.topic, type=AgentEvent)
        self._prefixes = [subtree_prefix(place.topic)] if subtree else []
        self._converter = client.data_converter.payload_converter
        self._shown: dict[str, _Shown] = {}
        # Where the oldest open turn started, for the cursors this read gives. ``_open`` has the
        # turns this read saw start, by agent, and ``_carried`` the ones open before it.
        self._turn_start = position.turn_start
        self._open: dict[str, int] = {}
        self._carried = position.turn_start
        # The offset of the newest ``Superseded`` item given, by this read or the one before.
        self._given = self.start if position.past_superseded else None
        # The items at ``start`` that the read before this one gave.
        self._skipped: list[Superseded] = []
        # What the reads before this one gave of the turn, read at the first ``Superseded``.
        self._before: dict[str, _Shown] | None = None
        self._before_from = position.turn_start

    def subscribe(self, start: int | None, **options: Any) -> Subscription[AgentEvent]:
        return self._stream.subscribe(
            self._topic, topic_prefixes=self._prefixes, offset=start, **options
        )

    async def take(self, item: Any, next_offset: int | None) -> TurnItem | None:
        """The ``TurnItem`` for one item of the read, or ``None`` for an item to skip."""
        if isinstance(item, Record):
            event = self._event(item)
            own = item.topic == self._place.topic
            _note(self._shown, item, own, event)
            self._track_turns(item, event, own)
            cursor = _Position(next_offset or None, False, self._turn_start).encode()
            return TurnItem(event=event, offset=str(item.offset), cursor=cursor, own=own)
        if isinstance(item, Superseded):
            if item.offset == self._given:
                if item.offset == self.start and self._before is None:
                    self._skipped.append(item)
                return None
            self._given = item.offset
            marked = await self._marker(item)
            if marked is not None:
                marker, own = marked
                cursor = _Position(next_offset or None, True, self._turn_start).encode()
                return TurnItem(event=marker, offset="", cursor=cursor, own=own)
        return None

    def _track_turns(self, record: Record[Any], event: AgentEvent, own: bool) -> None:
        """Move ``_turn_start`` to the first record of the oldest turn that is still open."""
        kind = event.event.type
        if kind == AgentEventType.TURN_STARTED:
            # The Workflow Task that wrote ``turn_started`` can have written the end of the turn
            # before it, which its retry withdraws too.
            key = repr(record.producer)
            first = min(self._shown[key].offsets) if key in self._shown else record.offset
            self._open[event.agent_id] = first
        elif kind == AgentEventType.TURN_END:
            self._open.pop(event.agent_id, None)
            if own:
                # A subagent's turn comes inside its root's, so the turns that were open before
                # this read are over too.
                self._carried = None
        starts = [*self._open.values(), *([self._carried] if self._carried is not None else [])]
        if starts:
            self._turn_start = min(starts)

    def _event(self, record: Record[Any]) -> AgentEvent:
        data = record.data
        if isinstance(data, RawValue):
            [data] = self._converter.from_payloads([data.payload], [AgentEvent])
        if not isinstance(data, AgentEvent):
            # A Topic that a prefix matched is made at run time, so the read has no type for it
            # and gives the plain JSON.
            data = AgentEvent.model_validate(data)
        return data

    async def _marker(self, item: Superseded) -> tuple[AgentEvent, bool] | None:
        """The marker for one ``Superseded`` item and whether it is the read agent's.

        ``None`` when it withdraws nothing that a reader of this Topic was given, such as a
        Workflow Task retry that wrote no record.
        """
        offsets, newest = _withdraw(item, self._shown)
        if self.start is not None and self._before_from is not None:
            # A reader of this Topic got the turn's records before ``start`` from earlier reads.
            if self._before is None:
                self._before = await self._read_before(self._before_from, self.start)
            earlier, writer = _withdraw(item, self._before)
            offsets += earlier
            newest = newest or writer
        if not offsets or newest is None:
            return None
        writer = newest.event or self._event(newest.record)
        marker = AgentEvent(
            agent_id=writer.agent_id,
            turn_id=writer.turn_id,
            turn_number=writer.turn_number,
            timestamp=writer.timestamp,
            event=AttemptSuperseded(withdrawn_offsets=[str(o) for o in sorted(offsets)]),
        )
        return marker, newest.own

    async def _read_before(self, turn_start: int, start: int) -> dict[str, _Shown]:
        """The records of the turn before ``start`` that no ``Superseded`` item withdrew.

        It costs a read only when an attempt superseded another. The records of a Topic that
        a prefix matched stay encoded, and only the one that stamps a marker is decoded.
        """
        shown: dict[str, _Shown] = {}
        earlier = self.subscribe(turn_start, wait_new_records=False, type=RawValue)
        try:
            async for old in earlier:
                if old.offset >= start:
                    break
                if isinstance(old, Record):
                    _note(shown, old, old.topic == self._place.topic, None)
                elif isinstance(old, Superseded):
                    _withdraw(old, shown)
        finally:
            await earlier.aclose()
        for given in self._skipped:
            _withdraw(given, shown)
        return shown


@dataclass
class _Shown:
    """The records of one Producer that a read gave. ``Superseded.withdraws`` looks at the
    Producer only, so they are withdrawn together."""

    # The newest of them, whether it is the read agent's own, and its event once decoded.
    record: Record[Any]
    own: bool
    event: AgentEvent | None
    offsets: list[int]


def _note(
    shown: dict[str, _Shown], record: Record[Any], own: bool, event: AgentEvent | None
) -> None:
    if record.producer is None:
        return
    key = repr(record.producer)
    offsets = shown[key].offsets if key in shown else []
    offsets.append(record.offset)
    shown[key] = _Shown(record, own, event, offsets)


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


async def _ended(client: Client, workflow_id: str, owner: tuple[str | None, str | None]) -> bool:
    """Whether the read's agent or the Stream's owner chain is known to have ended."""
    for wid, first_run_id in ((workflow_id, None), owner):
        if not wid:
            continue
        try:
            description = await client.get_workflow_handle(wid).describe()
        except RPCError as err:
            # A workflow that is gone writes nothing more. A transient error tells nothing about
            # it, so the read goes on and asks again after its next quiet stretch.
            if err.status == RPCStatusCode.NOT_FOUND:
                return True
            if err.status in _TRANSIENT:
                continue
            raise
        info = description.raw_description.workflow_execution_info
        if first_run_id and info.first_run_id and info.first_run_id != first_run_id:
            return True
        if description.status != WorkflowExecutionStatus.RUNNING:
            return True
    return False
