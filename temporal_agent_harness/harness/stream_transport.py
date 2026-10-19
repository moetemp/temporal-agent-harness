# ABOUTME: How the harness reaches its streams: the one place a store is named, the batched
# publisher an activity uses, and the readers every client-side consumer of turn events shares.
# The store is registered once on the client. Every context then asks for its stream the same
# way, and workflow code publishes through ``stream_producer`` and never names a store.

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from typing import Any

from temporalio.client import Client
from temporalio.contrib.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    FollowPosition,
    StreamProducer,
    StreamRecord,
    StreamRef,
    StreamStorePlugin,
    StreamTopic,
    Supersession,
    stream_handle,
    stream_producer,
)

from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS,
    AgentEvent,
    AttemptSuperseded,
)

STORE_ENV = "STREAMS_STORE"
REDIS_URL_ENV = "STREAMS_REDIS_URL"
DEFAULT_STORE = "redis"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379"
# How long ``check_turn_cursor`` waits for the store's answer on a token.
_CURSOR_CHECK = timedelta(milliseconds=200)


def store_name(default: str = DEFAULT_STORE) -> str:
    """The store this process names, read from ``STREAMS_STORE``."""
    return os.environ.get(STORE_ENV, default)


def store_from_env(default: str = DEFAULT_STORE) -> StreamStorePlugin:
    """A stream store built from ``STREAMS_STORE``.

    ``redis`` is the default, at ``STREAMS_REDIS_URL``. The web app, the Nexus adapter and the
    workers are separate processes, and they must all reach one store. ``memory`` keeps the
    streams in this process, so it suits tests and a single process only.

    Register the result once, as ``Client.connect(..., plugins=[store])``. Workers built from
    that client take it, an activity reaches it through ``stream_producer`` and any other
    code through ``stream_handle(client, ...)``. One store serves one client connection,
    so a second connection needs a store of its own.
    """
    name = store_name(default)
    if name == "redis":
        from temporalio.contrib.streams.redis import RedisStreams

        return RedisStreams(os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL))
    if name == "memory":
        from temporalio.contrib.streams.memory import MemoryStreams

        return MemoryStreams()
    raise ValueError(f"{STORE_ENV}={name!r} names no stream store")


def link_name(handle: str, subagent_turn: int) -> str:
    """The name of the link that one subagent turn opens and joins on the parent's stream."""
    return f"{handle}#{subagent_turn}"


def cursor(token: str) -> Cursor:
    """The cursor a stored token names, or the beginning when the token is empty."""
    return Cursor(token) if token else BEGINNING


async def latest_turn_event(client: Client, workflow_id: str) -> str:
    """The token of ``workflow_id``'s newest turn event, or empty when it has none.

    A client about to send a message reads this first, then follows the stream after it, so
    it sees the turn it started without replaying the agent's history. ``client`` carries the
    stream store as a plugin.
    """
    handle = stream_handle(client, workflow_id)
    return (await handle.latest(topic=TURN_EVENTS)).token


async def check_turn_cursor(
    client: Client, workflow_id: str, after: str, *, state: str = ""
) -> None:
    """Raise ``StreamCursorError`` now if the store refuses ``after`` on ``workflow_id``.

    With ``state``, the follow state of a unified stream, a follow checks both. A read checks
    its cursor at its first step, not at the call. A caller that must refuse a bad token before
    it starts a response, such as an SSE route, calls this first. The stream runtime refuses a
    token before it waits for records, so a short first step is enough. If no record follows
    ``after``, the step waits for ``_CURSOR_CHECK`` and the token is good.
    """
    if not (after or state):
        return
    handle = stream_handle(client, workflow_id)
    records: AsyncGenerator[Any, None] = (
        handle.follow(topic=TURN_EVENTS, after=FollowPosition(after, state))
        if state
        else handle.read(topic=TURN_EVENTS, after=cursor(after))
    )
    try:
        await asyncio.wait_for(anext(records), _CURSOR_CHECK.total_seconds())
    except (TimeoutError, StopAsyncIteration):
        pass
    finally:
        await records.aclose()


def follow_turn_events(
    client: Client, workflow_id: str, *, after: str = ""
) -> AsyncGenerator[StreamRecord[AgentEvent], None]:
    """Yield ``workflow_id``'s turn events after the record ``after`` names, then tail live.

    Data records come out, and so does the supersession a reader synthesizes when a streaming
    activity is retried: an activity that streams half an answer and then fails leaves those
    records in the stream, and its retry writes different words, so a consumer that rendered
    the first half has to be told. A producer's finish marker does not come out. Each record
    carries the cursor to store for a later ``after``. A token from another store or stream is
    refused with ``StreamCursorError`` at the first iteration, before anything is read. Close
    the generator to stop the read. ``client`` carries the stream store as a plugin.
    """
    handle = stream_handle(client, workflow_id)
    records = handle.read(topic=TURN_EVENTS, after=cursor(after))
    return _turn_records(records)


async def _turn_records(
    records: AsyncGenerator[StreamRecord[AgentEvent], None],
) -> AsyncGenerator[StreamRecord[AgentEvent], None]:
    """Data and supersession records, every one of them carrying an event."""
    previous: AgentEvent | None = None
    try:
        async for record in records:
            event = turn_event(record, previous)
            if event is None:
                continue
            if record.kind is RecordKind.DATA:
                previous = event
                yield record
            else:
                yield replace(record, value=event)
    finally:
        await records.aclose()


def turn_event(record: StreamRecord[Any], previous: AgentEvent | None) -> Any:
    """The event ``record`` gives a turn-event consumer, or ``None`` when it gives none.

    A supersession is synthesized with no value of its own, and it names a producer and an
    attempt rather than a turn. Stamping it with the envelope of ``previous``, the last event
    the same stream gave, is what makes it actionable: those are the records the retired
    attempt wrote. A data record gives its value as it was decoded. Link records and finish
    markers give nothing.
    """
    if record.kind is RecordKind.DATA:
        # A stale record came from an older attempt after a newer one was delivered, so it is
        # withdrawn as it arrives.
        return None if record.stale else record.value
    if record.kind is RecordKind.SUPERSEDED:
        # Nothing was delivered on this stream yet, so the retired attempt's records are
        # behind the caller's own start point and it holds none of them.
        if previous is None or record.supersession is None:
            return None
        return _superseded_event(previous, record.supersession)
    return None


def _superseded_event(previous: AgentEvent, supersession: Supersession) -> AgentEvent:
    """The marker for one supersession, in the turn whose records it retires."""
    return AgentEvent(
        agent_id=previous.agent_id,
        turn_id=previous.turn_id,
        turn_number=previous.turn_number,
        timestamp=previous.timestamp,
        event=AttemptSuperseded(
            producer_id=supersession.producer_id,
            superseded_attempt=supersession.previous_attempt,
            attempt=supersession.attempt,
        ),
    )


class ActivityPublisher:
    """A synchronous ``publish`` over the asynchronous stream producer.

    Activities publish from places that cannot await, such as an SDK's streaming callback,
    so ``publish`` only queues. A background task appends what has queued every
    ``batch_interval``, and leaving the context appends the tail, so nothing is lost and
    nothing is sent one record at a time.
    """

    def __init__(self, producer: StreamProducer, *, batch_interval: timedelta) -> None:
        self._producer = producer
        self._interval = batch_interval.total_seconds()
        self._pending: list[Any] = []
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        # Closing wakes the flusher rather than cancelling it: a cancel landing inside an
        # append would unwind ``flush`` with the batch it had already taken off ``_pending``.
        self._wake = asyncio.Event()

    def publish(self, value: Any) -> None:
        if self._closed:
            raise RuntimeError("publisher is closed")
        self._pending.append(value)

    async def flush(self) -> None:
        batch, self._pending = self._pending, []
        if batch:
            await self._producer.append(*batch)

    async def link(self, ref: StreamRef, *, name: str, after: str = "") -> None:
        """Append what has queued, then a ``LINK`` that opens ``ref``'s stream after ``after``.

        The link goes on the same producer right after the queued records, so a follower
        reads the stream it opens after them.
        """
        await self.flush()
        await self._producer.link(ref, name=name, after=Cursor(after) if after else None)

    async def _run(self) -> None:
        while not self._closed:
            try:
                await asyncio.wait_for(self._wake.wait(), self._interval)
            except TimeoutError:
                pass
            await self.flush()

    async def __aenter__(self) -> ActivityPublisher:
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self._closed = True
        self._wake.set()
        if self._task is not None:
            await self._task
        # The tail goes out even when the activity is failing, so a reader sees what was
        # produced up to the failure.
        await self.flush()


@asynccontextmanager
async def publisher_for_activity(
    topic: str | StreamTopic[Any], *, batch_interval: timedelta = timedelta(milliseconds=50)
) -> AsyncIterator[ActivityPublisher]:
    """A batched publisher onto ``topic`` of the stream this activity's workflow publishes.

    The handle is the stream of the workflow that scheduled this activity, through the store
    the worker took from its client. The producer carries the activity's own id and attempt,
    so a retry's records deduplicate and a new attempt is reported to readers as a
    supersession, and the records land next to the workflow's own for whoever follows the
    topic.
    """
    producer = stream_producer(topic)
    async with ActivityPublisher(producer, batch_interval=batch_interval) as publisher:
        yield publisher
