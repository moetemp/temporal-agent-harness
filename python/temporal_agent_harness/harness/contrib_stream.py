# ABOUTME: The agent event stream on the SDK's Streams API, which an agent picks with
# ``stream=ContribStream()``. An agent picks Workflow Streams with ``stream=WorkflowStream()``.
#
# An agent tree shares ONE Stream, which the root agent creates. Every agent publishes its turn
# events there, so one read of the Stream gives every agent's events in one order. A reader keeps
# one agent's events, or its subtree's, by ``AgentEvent.agent_id``. Workflow code writes the
# Stream with one append at a time, through a Local Activity of the harness
# (``contrib_stream_activities``). An Activity writes it through an ``AppendBuffer``, and a client
# reads it through ``Client.get_stream``.
#
# Only this module, ``_contrib_stream_parts`` and the bodies of the Local Activities import the
# Streams API. The rest of the harness imports them lazily, so an agent on ``WorkflowStream()``
# runs on an SDK without that API.

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from urllib.parse import quote

from temporalio import activity, workflow
from temporalio.api.enums.v1 import StreamErrorReason
from temporalio.client import Client, StreamStore, WorkflowExecutionStatus
from temporalio.common import RetryPolicy
from temporalio.exceptions import FailureError, StreamError
from temporalio.nexus.system.stream_service import (
    Producer,
    ProducerKindLocalActivity,
    ReadRecordsResponse,
    StreamRecord,
    StreamRef,
)
from temporalio.nexus.system.stream_service.client import Stream
from temporalio.service import RPCError, RPCStatusCode

from temporal_agent_harness.harness._contrib_stream_parts import (
    MAX_APPEND_RECORDS,
    AppendBuffer,
    Attempts,
    ref_from_data,
    ref_to_data,
)
from temporal_agent_harness.harness.agent_protocol import (
    AgentEvent,
    AgentEventType,
    AgentStream,
    AttemptSuperseded,
)
from temporal_agent_harness.harness.contrib_stream_activities import (
    NO_STORE,
    append_events,
    create_event_stream,
    has_store,
)
from temporal_agent_harness.harness.stream_context import StreamBackend, TurnStreamContext

STORE_ENV = "STREAMS_STORE"
REDIS_URL_ENV = "STREAMS_REDIS_URL"
KEY_PREFIX_ENV = "STREAMS_KEY_PREFIX"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379"
# How long a read waits with no Record before it asks whether the agent still runs. The end of a
# run closes nothing, so the read has to find out.
_LIVENESS_CHECK = timedelta(seconds=5)
# The first wait of a read for a Stream whose create is in flight. Each later wait doubles, up to
# the liveness check.
_CREATE_RETRY = timedelta(milliseconds=50)
# The read errors of a cursor that is not a position on the Stream. An offset of the wrong
# length, such as one longer than the Store's, is invalid.
_BAD_CURSOR = frozenset(
    {
        StreamErrorReason.STREAM_ERROR_REASON_OFFSET_TOO_HIGH,
        StreamErrorReason.STREAM_ERROR_REASON_EXPIRED,
        StreamErrorReason.STREAM_ERROR_REASON_INVALID,
    }
)
_NOT_FOUND = StreamErrorReason.STREAM_ERROR_REASON_NOT_FOUND
# The errors of that check that are transient.
_TRANSIENT = frozenset(
    {
        RPCStatusCode.UNAVAILABLE,
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.RESOURCE_EXHAUSTED,
        RPCStatusCode.ABORTED,
    }
)
# Every client of this process shares one memory Store, so they all see the same Streams.
_memory: StreamStore | None = None
# The options of the Local Activities that write for Workflow code. A Store outage holds the run
# and loses no event, so a call retries with no limit until the Store is back. An attempt that
# wrote and then timed out writes again, as a higher epoch of its Local Activity, so a reader
# withdraws the first copy. Every other Stream error is non-retryable and fails at once.
_LOCAL_ACTIVITY_TIMEOUT = timedelta(seconds=60)
_HOLD_ON_OUTAGE = RetryPolicy(maximum_attempts=0)


_STREAM_NAME = "agent_events"


class TurnCursorError(ValueError):
    """A turn cursor that is not a position on the agent's Stream."""


def stream_stores_from_env() -> list[StreamStore]:
    """The Store that ``STREAMS_STORE`` names, as the list for ``stream_stores=``. Empty when unset.

    ``redis`` is the Redis at ``STREAMS_REDIS_URL``. The web app, the Nexus adapter and the
    workers are separate processes, and they must all reach one Redis. ``STREAMS_KEY_PREFIX``
    puts the keys under a prefix of their own, which keeps two deployments (or two test runs)
    on one Redis apart. ``memory`` keeps the Streams in this process, for tests and a single
    process only. Unset means the process serves agents on ``WorkflowStream()`` only.

    Register the result as ``Client.connect(..., stream_stores=stores)``. Workers built from
    that client take it. The list has one Store, so a place found without asking the agent
    (``root_place``) is on it. Every call gives the one memory Store, so all clients of the
    process see the same Streams.

    The Store needs no retry setting for Workflow code. The Local Activities of the harness that
    write for it retry with no limit, so a Store outage holds the run until the Store is back,
    and loses no event. The Redis Store sets no ``last_write_ttl``, so it keeps an idle session's
    Stream. Build a Store of your own to set one.
    """
    global _memory
    store = os.environ.get(STORE_ENV)
    if not store:
        return []
    if store == "memory":
        from temporalio.testing import MemoryStreamStore

        if _memory is None:
            _memory = MemoryStreamStore()
        return [_memory]
    if store != "redis":
        raise ValueError(f"{STORE_ENV}={store!r} is not a Stream Store")
    from temporalio.contrib.redis.streams import RedisStreamStore

    return [
        RedisStreamStore(
            os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL),
            key_prefix=os.environ.get(KEY_PREFIX_ENV) or None,
        )
    ]


class ContribStream(StreamBackend):
    """Selects the SDK's Streams API for an agent tree's turn events.

    Pass ``stream=ContribStream()`` to ``AgentWorkflowRunner``. The root agent creates the
    tree's Stream on the Worker's Store, and its subagents publish there too, whatever their
    own ``stream=`` says. The Worker's client needs a Redis or memory Stream Store, such as
    the one ``stream_stores_from_env`` gives, and the Worker needs the Local Activities that
    ``AgentHarnessPlugin`` registers. The harness never closes the Stream, so the
    Store keeps every Record until its ``last_write_ttl`` after the last write, or for good
    with none. Its Retention, which counts from a close, doesn't apply.
    """

    def open_sink(self) -> ContribSink:
        return open_root()


def _store_name(client: Client) -> str:
    """The name of the client's Store. A client takes at most one."""
    if not has_store(client):
        raise RuntimeError(NO_STORE)
    return client.config(active_config=True)["stream_stores"][0].name


# ---------------------------------------------------------------------------
# Workflow side
# ---------------------------------------------------------------------------


class ContribSink:
    """What the runner publishes through on a Stream of the SDK's Streams API.

    ``publish`` puts the event in a queue. One writer task takes everything queued and writes it
    as one append, through the harness's Local Activity ``append_events``, then loops. So one
    append is in flight at a time, the Records keep the order of ``publish``, and a burst of
    events costs one Local Activity. Each append gives the offset of its last Record, which
    tells the runner where its own Records reached.

    If the Store refuses an append, the live view loses those events, the sink logs how many,
    and the agent continues. A Store outage is not a refusal. The Local Activity retries until
    the Store is back, the events after it wait, and so does the end of the run.
    """

    def __init__(self, ref: dict[str, Any] | None, place: AgentStream | None) -> None:
        # The Stream's ref as ``ref_to_data`` writes it, which the Local Activities take.
        self._ref = ref
        # A root's sink has no place until its create returns, so nothing gives the place out
        # before the Stream exists.
        self.place = place
        # The offset of the newest Record that this run chain wrote. A run after continue-as-new
        # starts from the one that ``continue_as_new_config`` passed on.
        self._newest = parse_cursor(place.cursor) if place is not None else None
        # The events that wait for the writer, and the writer while it runs.
        self._queue: list[AgentEvent] = []
        self._writer: asyncio.Task[None] | None = None
        # A root's create, and the id of the Stream it makes. The events published before it
        # returns wait in the queue.
        self._opening: asyncio.Future[None] | None = None
        self._root_id: str | None = None

    def publish(self, event: AgentEvent) -> None:
        self._queue.append(event)
        self._wake()

    async def opened(self) -> None:
        """Wait until the Stream exists, and raise the create's error."""
        if self._opening is not None:
            await self._opening

    async def settled(self) -> None:
        """Wait until the queue is empty and no append is in flight."""
        if self._opening is not None:
            await workflow.wait([self._opening])
        while self._writer is not None:
            await workflow.wait([self._writer])

    def place_before_create(self) -> AgentStream | None:
        """A root's place while its create is in flight, for a status only.

        A query can run between the Activations of the Workflow Task that waits for the create.
        A status with no place sends a reader to Workflow Streams, so it names the Stream id,
        which is known already. The reader waits for the create. The ref has no Store name
        until the create gives one, and a client routes it to its own Store.
        """
        if self.place is not None or self._root_id is None:
            return self.place
        return AgentStream(ref={"id": self._root_id, "store": None})

    def accepted_offset(self) -> int:
        return 0

    def accepted_cursor(self) -> str:
        return self._newest.hex() if self._newest is not None else ""

    def child_place(self, agent_id: str) -> AgentStream:
        assert self.place is not None, "the runner waits for opened() first"
        return AgentStream(ref=self.place.ref, agent_id=agent_id)

    def activity_ref(self) -> dict[str, Any]:
        assert self._ref is not None, "the runner waits for opened() first"
        return dict(self._ref)

    async def _open(self, stream_id: str) -> None:
        ref = await workflow.execute_local_activity(
            create_event_stream,
            stream_id,
            start_to_close_timeout=_LOCAL_ACTIVITY_TIMEOUT,
            retry_policy=_HOLD_ON_OUTAGE,
        )
        self._ref = ref
        self.place = AgentStream(ref=dict(ref))
        self._wake()

    def _wake(self) -> None:
        if self._ref is not None and self._writer is None and self._queue:
            self._writer = asyncio.create_task(self._write(self._ref))

    async def _write(self, ref: dict[str, Any]) -> None:
        try:
            while self._queue:
                batch = self._queue[:MAX_APPEND_RECORDS]
                del self._queue[:MAX_APPEND_RECORDS]
                try:
                    last = await workflow.execute_local_activity(
                        append_events,
                        args=[ref, batch],
                        start_to_close_timeout=_LOCAL_ACTIVITY_TIMEOUT,
                        retry_policy=_HOLD_ON_OUTAGE,
                    )
                except FailureError as err:
                    # A refused append costs its events and not the run.
                    workflow.logger.warning(
                        "%d agent events were not written to their Stream: %s",
                        len(batch),
                        err.cause or err,
                    )
                    continue
                offset = bytes.fromhex(last)
                if offset and (self._newest is None or offset > self._newest):
                    self._newest = offset
        finally:
            self._writer = None


def open_root() -> ContribSink:
    """Start the create of the tree's Stream in a root agent's ``@agent.init``.

    The harness's Local Activity ``create_event_stream`` creates the Stream, or finds the one
    that exists. The Stream id holds the workflow id and the first run id, so a retry run, or a
    run after a plain ``workflow.continue_as_new``, gets the Stream of the first run. A write to
    a Stream that nobody created fails at once, so the runner waits for ``opened()`` before it
    gives the place out, in a reply, a subagent or an Activity. A status can't wait, so it names
    the Stream before the create returns, and its reader waits. With no Store on the Worker's
    client, the create fails with a non-retryable error that names the missing setup, and so
    does the run.

    Nothing closes the Stream. The end of the run chain closes nothing, and the harness never
    closes it, so an idle session keeps its Stream. The Store deletes an open Stream after its
    ``last_write_ttl`` with no write, if it sets one. A reader checks whether the run has ended.
    """
    info = workflow.info()
    sink = ContribSink(None, None)
    sink._root_id = _stream_id(info.workflow_id, info.first_execution_run_id)
    sink._opening = asyncio.ensure_future(sink._open(sink._root_id))
    return sink


def open_place(place: AgentStream) -> ContribSink:
    """Write the Stream at ``place``, for a subagent or for a run after continue-as-new."""
    return ContribSink(ref_to_data(ref_from_data(place.ref)), place)


def root_place(client: Client, workflow_id: str, first_run_id: str) -> AgentStream:
    """The place of the root agent ``workflow_id``, found without asking the agent.

    The root agent picks its Stream id from its workflow id and first run id, so the ref needs
    no call. Only the agent's status gives its agent id, so this place has none. It is on the
    client's Store, which raises ``RuntimeError`` when the client has none.
    """
    ref: StreamRef[AgentEvent] = StreamRef(
        id=_stream_id(workflow_id, first_run_id), store=_store_name(client)
    )
    return AgentStream(ref=ref_to_data(ref))


def _stream_id(workflow_id: str, first_run_id: str) -> str:
    # ``AgentStream.owner`` reads the two ids back. The first run id keeps a reused workflow id
    # from sharing a Stream with the chain before it.
    return ":".join(quote(part, safe="") for part in (workflow_id, first_run_id, _STREAM_NAME))


# ---------------------------------------------------------------------------
# Activity side
# ---------------------------------------------------------------------------


@asynccontextmanager
async def activity_publisher(
    context: TurnStreamContext, *, max_delay: timedelta
) -> AsyncIterator[AppendBuffer[AgentEvent]]:
    """Buffer an Activity's events for the Stream in ``context``.

    The buffer's ``publish`` is the one that ``TurnEventPublisher`` calls. Leaving the block
    writes the tail, also when the Activity fails, so a reader sees what the attempt produced
    up to the failure.
    """
    assert context.ref is not None
    client = activity.client()
    _store_name(client)
    # The SDK writes as this attempt's Producer, so a reader can tell a retry's Records from
    # those of the failed attempt.
    stream = client.get_stream(ref_from_data(context.ref), body_type=AgentEvent)
    async with AppendBuffer[AgentEvent](stream.append, max_delay=max_delay) as events:
        yield events


# ---------------------------------------------------------------------------
# Client side
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TurnItem:
    """One event a read gives, with where it is on the Stream."""

    event: AgentEvent
    # The Record's own offset, in hex. A marker has none, since it comes just before the first
    # Record of the next attempt, and a client tells events apart by their offsets.
    offset: str
    # The cursor that resumes just past this item.
    cursor: str
    # Whether the agent that the read is for published it, and not a subagent below it.
    own: bool


def parse_cursor(cursor: str) -> bytes | None:
    """The offset in a cursor, or ``None`` for the beginning.

    A cursor is the offset of the last Record read, in hex. A read resumes after it. A cursor
    that a read gives can say more after the offset (``_Position``).
    """
    return _position(cursor).offset


# ``<offset>``, then ``.s`` when the marker before the next Record was given, then
# ``.t<offset>`` with where the oldest open turn started.
_CURSOR = re.compile(r"([0-9a-f]+)(\.s)?(?:\.t([0-9a-f]*))?", re.ASCII)


@dataclass(frozen=True)
class _Position:
    """Where a read resumes, and what it needs to know of the reads before it."""

    offset: bytes | None
    # A marker comes just before the Record of the later epoch, at the same position. So a read
    # that resumes past the marker resumes before that Record, and skips the marker.
    past_superseded: bool = False
    # Where a read gives the first Record of the Workflow Task that started the oldest turn still
    # open, empty for the beginning. A retry withdraws Records of its own turn only, so a later
    # read looks back no further.
    turn_start: bytes | None = None

    def encode(self) -> str:
        if self.offset is None:
            return ""
        cursor = self.offset.hex()
        if self.past_superseded:
            cursor += ".s"
        if self.turn_start is not None:
            cursor += f".t{self.turn_start.hex()}"
        return cursor


def _position(cursor: str) -> _Position:
    if not cursor:
        return _Position(None)
    parsed = _CURSOR.fullmatch(cursor)
    if parsed is None:
        raise TurnCursorError(f"not a turn cursor: {cursor!r}")
    try:
        offset = bytes.fromhex(parsed[1])
        turn_start = bytes.fromhex(parsed[3]) if parsed[3] is not None else None
    except ValueError:
        raise TurnCursorError(f"not a turn cursor: {cursor!r}") from None
    if turn_start is not None and turn_start > offset:
        raise TurnCursorError(f"not a turn cursor: {cursor!r}")
    return _Position(offset, parsed[2] is not None, turn_start)


def _at(offset: bytes | None) -> bytes | None:
    # A read from the beginning starts at the empty offset, which a cursor has as no offset.
    return offset or None


def _client_stream(client: Client, place: AgentStream) -> Stream[AgentEvent]:
    _store_name(client)
    return client.get_stream(ref_from_data(place.ref), body_type=AgentEvent)


async def newest_offset(client: Client, place: AgentStream) -> bytes | None:
    """The offset of the agent's newest Record on its Stream, or ``None`` when it has none."""
    try:
        described = await _client_stream(client, place).describe()
    except StreamError as err:
        if err.reason != _NOT_FOUND:
            raise
        return None
    return described.last_offset or None


async def check_turn_cursor(client: Client, place: AgentStream, after: str) -> None:
    """Raise ``TurnCursorError`` at once if ``after`` is not a position on the agent's Stream.

    A caller that must refuse a bad cursor before it starts a response, such as an SSE route,
    calls this first. It reads at most one Record and does not wait for new ones.
    """
    offset = parse_cursor(after)
    if offset is None:
        return
    try:
        await _client_stream(client, place).read(from_offset=offset, max_records=1)
    except StreamError as err:
        # A Stream that does not exist has no position past its beginning, and an expired
        # Stream has no Records left.
        if err.reason not in _BAD_CURSOR and err.reason != _NOT_FOUND:
            raise
        raise TurnCursorError(str(err)) from err


async def read_turn_events(
    client: Client,
    place: AgentStream,
    *,
    workflow_id: str,
    after: str = "",
    subtree: bool = False,
    on_cursor: Callable[[str], None] | None = None,
) -> AsyncGenerator[TurnItem, None]:
    """Yield an agent's turn events from the cursor ``after``, then tail live.

    ``place`` is the agent's, from its status or a send's reply. The whole tree writes one
    Stream, so the read keeps the events of ``place.agent_id``. With ``subtree``, the events
    of every subagent below the agent come too, in the one order of the shared Stream.

    An attempt that supersedes Records of an earlier attempt that this read gave, or that an
    earlier read gave in the same turn, comes out as an ``AttemptSuperseded`` event with their
    offsets, just before its first Record. The read ends at the Stream's close, and when
    ``workflow_id`` or the Stream's owner no longer runs, since the end of a run closes nothing.
    A malformed cursor, one that is no position on the Stream, or one on an expired Stream,
    raises ``TurnCursorError``.

    ``on_cursor`` gets the cursor past each item that the read takes, also past a Record that
    it skips, such as another agent's. A later read can start there.
    """
    reading = _Read(client, place, after=after, subtree=subtree)
    owner = place.owner()

    async def items(
        pages: AsyncGenerator[ReadRecordsResponse[AgentEvent], None],
    ) -> AsyncGenerator[TurnItem | ReadRecordsResponse[AgentEvent], None]:
        """Each item of the pages, then the last page, which tells whether the read ended."""
        try:
            async for page in pages:
                for record in page.records:
                    taken = await reading.take(record)
                    if not taken and on_cursor is not None:
                        on_cursor(reading.cursor())
                    for item in taken:
                        if on_cursor is not None:
                            on_cursor(item.cursor)
                        yield item
                yield page
        finally:
            await pages.aclose()

    async def follow() -> AsyncGenerator[TurnItem, None]:
        # A page waits up to the liveness check for a new Record. An empty page is that much
        # silence, so the read asks whether the agent still runs.
        tail = items(reading.pages(wait=_LIVENESS_CHECK))
        try:
            async for item in tail:
                if isinstance(item, TurnItem):
                    yield item
                elif item.done:
                    return
                elif not item.records and await _ended(client, workflow_id, owner):
                    break
        finally:
            await tail.aclose()
        # The Records that the run wrote just before it ended can still arrive, so the read
        # continues to the newest Record and stops there.
        rest = items(reading.pages(wait=None))
        try:
            async for item in rest:
                if isinstance(item, TurnItem):
                    yield item
        finally:
            await rest.aclose()

    delay = _CREATE_RETRY.total_seconds()
    while True:
        following = follow()
        try:
            async for item in following:
                yield item
            return
        except StreamError as err:
            if err.reason in _BAD_CURSOR:
                raise TurnCursorError(str(err)) from err
            if err.reason != _NOT_FOUND:
                raise
            # A status names a root's Stream before its create returns, so a Stream that
            # doesn't exist yet is one whose create is in flight. The read waits for it while
            # the agent runs, as it waits for new Records. A Stream that never came is one whose
            # agent ended before its create, with nothing to give.
            if reading.taken or await _ended(client, workflow_id, owner):
                return
        finally:
            await following.aclose()
        await asyncio.sleep(delay)
        delay = min(delay * 2, _LIVENESS_CHECK.total_seconds())


def _task(producer: Producer | None) -> tuple[str, int, int] | None:
    """The run and Workflow Task of a Local Activity Producer, such as the harness's append."""
    if producer is None or not isinstance(producer.kind, ProducerKindLocalActivity):
        return None
    local = producer.kind.value
    return (local.run_id, local.epoch.task.started_event_id, local.epoch.task.attempt)


@dataclass(frozen=True)
class _Shown:
    """A Record that a read gave, and whether it is the read agent's own."""

    event: AgentEvent
    own: bool


class _Read:
    """One read of an agent's events, and what it gave, so a marker can tell what it withdraws."""

    def __init__(self, client: Client, place: AgentStream, *, after: str, subtree: bool) -> None:
        position = _position(after)
        self.start = position.offset
        self._stream = _client_stream(client, place)
        self._agent_id = place.agent_id
        self._subtree = subtree
        # The offset of the last Record taken. The next page starts after it. Each Workflow Task
        # keeps the position before its first Record, so that a turn's lookback covers every
        # Record of the Task that started it.
        self._position = self.start or b""
        self._task_starts: dict[tuple[str, int, int], bytes] = {}
        # Where the oldest open turn started, for the cursors this read gives. ``_open`` has the
        # turns this read saw start, by agent, and ``_carried`` the ones open before it.
        self._turn_start = position.turn_start
        self._open: dict[str, bytes] = {}
        self._carried = position.turn_start
        # The read before this one gave the marker before the first Record after ``start``.
        self._past_superseded = position.past_superseded
        # The Records given, by Producer epoch. The first Record that this read gives first
        # notes what the reads before it gave of the open turn.
        self._attempts: Attempts[_Shown] = Attempts()
        # Whether the read took a Record, so a later ``NOT_FOUND`` is not a create in flight.
        self.taken = False
        self._look_back_from = position.turn_start if self.start is not None else None

    def pages(
        self, *, wait: timedelta | None
    ) -> AsyncGenerator[ReadRecordsResponse[AgentEvent], None]:
        """The pages after the last Record taken. With no ``wait``, they end at the newest."""
        return self._stream.iter_read(from_offset=self._position or None, wait=wait)

    def cursor(self) -> str:
        """The cursor past a Record that the read skipped."""
        return _Position(_at(self._position), False, self._turn_start).encode()

    def _own(self, event: AgentEvent) -> bool | None:
        """Whether the read's agent published ``event``, or ``None`` for an event it skips.

        A subagent's id is its parent's id plus one segment. A place with no agent id is a
        root's, whose id is one segment.
        """
        agent_id = event.agent_id
        if self._agent_id is None:
            own, below = "-" not in agent_id, "-" in agent_id
        else:
            own, below = agent_id == self._agent_id, agent_id.startswith(f"{self._agent_id}-")
        if own or (self._subtree and below):
            return own
        return None

    async def take(self, record: StreamRecord[AgentEvent]) -> list[TurnItem]:
        """The items for one Record: its marker if it supersedes Records that a read gave, then
        the Record itself. None for a Record to skip."""
        self.taken = True
        first, self._past_superseded = self._past_superseded, False
        before, self._position = self._position, record.offset
        task = _task(record.producer)
        if task is not None:
            self._task_starts.setdefault(task, before)
        event = record.body
        own = self._own(event)
        if own is None:
            return []
        if self._look_back_from is not None:
            look_back_from, self._look_back_from = self._look_back_from, None
            await self._look_back(look_back_from)
        items: list[TurnItem] = []
        withdrawn = self._attempts.see(record.producer, record.offset, _Shown(event, own))
        # The read before this one gave the marker, so this one gives only the Record.
        if withdrawn and not first:
            items.append(self._marker(withdrawn, before))
        self._track_turns(record, event, own, before)
        cursor = _Position(_at(record.offset), False, self._turn_start).encode()
        items.append(TurnItem(event=event, offset=record.offset.hex(), cursor=cursor, own=own))
        return items

    def _track_turns(
        self, record: StreamRecord[AgentEvent], event: AgentEvent, own: bool, before: bytes
    ) -> None:
        """Move ``_turn_start`` to the start of the oldest turn still open."""
        kind = event.event.type
        if kind == AgentEventType.TURN_STARTED:
            # The Workflow Task that wrote ``turn_started`` can have written the end of the turn
            # before it, which its retry withdraws too.
            task = _task(record.producer)
            self._open[event.agent_id] = self._task_starts.get(task, before) if task else before
        elif kind == AgentEventType.TURN_END:
            self._open.pop(event.agent_id, None)
            if own:
                # A subagent's turn comes inside its root's, so the turns that were open before
                # this read are over too.
                self._carried = None
        starts = [*self._open.values(), *([self._carried] if self._carried is not None else [])]
        if starts:
            self._turn_start = min(starts)

    def _marker(self, withdrawn: list[tuple[bytes, _Shown]], before: bytes) -> TurnItem:
        """The marker of the Records that a later epoch withdraws, at the position ``before``
        its first Record. The newest withdrawn Record gives its agent and turn."""
        offsets = sorted(offset for offset, _ in withdrawn)
        newest = max(withdrawn, key=lambda found: found[0])[1]
        writer = newest.event
        marker = AgentEvent(
            agent_id=writer.agent_id,
            turn_id=writer.turn_id,
            turn_number=writer.turn_number,
            timestamp=writer.timestamp,
            event=AttemptSuperseded(withdrawn_offsets=[offset.hex() for offset in offsets]),
        )
        cursor = _Position(_at(before), True, self._turn_start).encode()
        return TurnItem(event=marker, offset="", cursor=cursor, own=newest.own)

    async def _look_back(self, turn_start: bytes) -> None:
        """Note the Records of the open turn up to ``start`` that the reads before this one gave,
        so a later epoch withdraws them too. It costs one read."""
        assert self.start is not None
        pages = self._stream.iter_read(from_offset=turn_start or None)
        try:
            async for page in pages:
                for old in page.records:
                    if old.offset > self.start:
                        return
                    own = self._own(old.body)
                    if own is not None:
                        self._attempts.see(old.producer, old.offset, _Shown(old.body, own))
        finally:
            await pages.aclose()


async def _ended(client: Client, workflow_id: str, owner: tuple[str | None, str | None]) -> bool:
    """Whether the read's agent or the Stream's owner chain is known to have ended."""
    for wid, first_run_id in ((workflow_id, None), owner):
        if not wid:
            continue
        try:
            description = await client.get_workflow_handle(wid).describe()
        except RPCError as err:
            # A workflow that is gone writes nothing more. A transient error tells nothing about
            # it, so the read continues and asks again after its next idle wait.
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
