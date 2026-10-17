# ABOUTME: How the harness reaches its streams: the one place a provider is named, the batched
# publisher an activity uses, and the readers every client-side consumer of turn events shares.
# The provider is registered once on the client; every context then asks for its stream the
# same way, and workflow code publishes through ``workflow.stream_writer`` and never names a
# store.

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.streams import (
    BEGINNING,
    Cursor,
    RecordKind,
    StreamProducer,
    StreamProvider,
    StreamRecord,
    StreamTopic,
    Supersession,
)

from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS,
    AgentEvent,
    AttemptSuperseded,
)

PROVIDER_ENV = "STREAMS_PROVIDER"
DEFAULT_PROVIDER = "workflow_streams"


def provider_name() -> str:
    """The provider this process names, read from ``STREAMS_PROVIDER``."""
    return os.environ.get(PROVIDER_ENV, DEFAULT_PROVIDER)


def provider_from_env() -> StreamProvider:
    """A stream provider built from ``STREAMS_PROVIDER``.

    ``workflow_streams`` (today's Workflow Streams, nothing to run) is the default. ``redis``
    reads ``AI198_REDIS_URL``; ``native`` needs a Temporal server that carries streams;
    ``memory`` is the in-process reference the conformance tests use. The Nexus front is not
    offered: it needs an endpoint name and an HTTP address the harness has no settings for,
    and it cannot serve a worker.

    Register the result once, as ``Client.connect(..., plugins=[provider])``. Workers built
    from that client inherit it, an activity reaches it through ``activity.stream_handle()``
    and any other code through ``client.get_stream_handle()``. Two providers share nothing,
    so a process that needs the same store in two places hands the same object to both.
    Close it with ``provider.close()`` when the process is done.
    """
    name = provider_name()
    if name == "workflow_streams":
        from temporalio.streams.providers.workflow_streams import WorkflowStreamsProvider

        # The shipped transport polls between deliveries; the UI wants deltas within a few
        # milliseconds of publish.
        return WorkflowStreamsProvider(poll_cooldown=timedelta(milliseconds=10))
    if name == "redis":
        from temporalio.streams.providers.redis import RedisStreams

        return RedisStreams(url=os.environ.get("AI198_REDIS_URL", "redis://127.0.0.1:6379"))
    if name == "native":
        from temporalio.streams.providers.native import NativeStreams

        return NativeStreams()
    if name == "memory":
        from temporalio.streams.providers.memory import MemoryStreams

        return MemoryStreams()
    raise ValueError(f"{PROVIDER_ENV}={name!r} names no stream provider")


def cursor(token: str) -> Cursor:
    """The cursor a stored token names, or the beginning when the token is empty."""
    return Cursor(token) if token else BEGINNING


async def latest_turn_event(client: Client, workflow_id: str) -> str:
    """The token of ``workflow_id``'s newest turn event, or empty when it has none.

    A client about to send a message reads this first, then follows the stream after it, so
    it sees the turn it started without replaying the agent's history. ``client`` carries the
    stream provider as a plugin.
    """
    handle = client.get_stream_handle(workflow_id)
    return (await handle.latest(topic=TURN_EVENTS)).token


def follow_turn_events(
    client: Client, workflow_id: str, *, after: str = ""
) -> AsyncGenerator[StreamRecord[AgentEvent], None]:
    """Yield ``workflow_id``'s turn events after the record ``after`` names, then tail live.

    Data records come out, and so does the supersession a reader synthesizes when a streaming
    activity is retried: an activity that streams half an answer and then fails leaves those
    records in the stream, and its retry writes different words, so a consumer that rendered
    the first half has to be told. A producer's finish marker does not come out. Each record
    carries the cursor to store for a later ``after``. A token another provider minted is
    refused by this call with ``StreamCursorError``, before anything is read. Closing the
    generator closes the subscription, which matters on the transport that parks a long poll
    against the workflow. ``client`` carries the stream provider as a plugin.
    """
    handle = client.get_stream_handle(workflow_id)
    records = handle.read(topic=TURN_EVENTS, after=cursor(after))
    return _turn_records(records)


async def _turn_records(
    records: AsyncGenerator[StreamRecord[AgentEvent], None],
) -> AsyncGenerator[StreamRecord[AgentEvent], None]:
    """Data and supersession records, every one of them carrying an event.

    A supersession is synthesized with no value of its own, and it names a producer and an
    attempt rather than a turn. Stamping it here with the envelope of the last event this
    stream produced is what makes it actionable: those are the records the retired attempt
    wrote. Every consumer then reads ``record.value`` the same way, whatever the kind.
    """
    previous: AgentEvent | None = None
    try:
        async for record in records:
            if record.kind is RecordKind.DATA:
                previous = record.value
                yield record
            elif record.kind is RecordKind.SUPERSEDED:
                # Nothing was delivered on this stream yet, so the retired attempt's records
                # are behind the caller's own start point and it holds none of them.
                if previous is None or record.supersession is None:
                    continue
                yield replace(record, value=_superseded_event(previous, record.supersession))
    finally:
        await records.aclose()


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
    """A synchronous ``publish`` over the interface's asynchronous producer.

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

    The handle is this activity's own workflow, pinned to its run, through the provider the
    worker inherited from its client. The producer carries the activity's own id and attempt,
    so a retry's records deduplicate and a new attempt is reported to readers as a
    supersession, and the records land next to the workflow's own for whoever follows the
    topic.
    """
    producer = activity.stream_handle().producer(topic=topic)
    async with ActivityPublisher(producer, batch_interval=batch_interval) as publisher:
        yield publisher
