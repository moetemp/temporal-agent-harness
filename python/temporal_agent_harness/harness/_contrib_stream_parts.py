# ABOUTME: The parts of ``contrib_stream`` that the SDK's Streams API leaves to its caller. They are
# a Stream ref as plain data, the write buffer of an Activity, and the rule of superseded attempts.
#
# Each part is small and has its own tests, so the reader and the writers in ``contrib_stream``
# stay about the agent's events.

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, TypeVar

from temporalio.nexus.system.stream_service import (
    Producer,
    ProducerKindLocalActivity,
    ProducerKindStandaloneActivity,
    ProducerKindWorkflowActivity,
    StreamRef,
    Timestamp,
)

T = TypeVar("T")

# The most Records that a Store takes in one append. A larger batch fails as too large.
MAX_APPEND_RECORDS = 1000


# ---------------------------------------------------------------------------
# A ref as data
# ---------------------------------------------------------------------------


def ref_to_data(ref: StreamRef[Any]) -> dict[str, Any]:
    """The plain dict that a place keeps for ``ref``. It crosses Workflow, Activity and client
    payloads as JSON."""
    return {"id": ref.id, "store": ref.store}


def ref_from_data(data: Mapping[str, Any]) -> StreamRef[Any]:
    """The ``StreamRef`` of a dict that ``ref_to_data`` wrote."""
    store = data.get("store")
    return StreamRef(id=str(data["id"]), store=str(store) if store is not None else None)


# ---------------------------------------------------------------------------
# The write buffer of an Activity
# ---------------------------------------------------------------------------


class AppendBuffer(Generic[T]):
    """Batches the writes of one Producer to one Stream.

    ``publish`` returns at once. The buffer writes what it holds as one append at most
    ``max_delay`` after the first of it, or at once when it holds a full append. One append is in
    flight at a time, so the appends keep the order of ``publish``. Use it in ``async with``,
    which writes the tail at the end of the block, also when the block raises.

    After an append fails, the buffer writes nothing more, so a reader sees no gap in what the
    Producer wrote. Each later ``publish`` and ``flush`` raises that error.
    """

    def __init__(
        self, append: Callable[..., Awaitable[object]], *, max_delay: timedelta
    ) -> None:
        self._append = append
        self._max_delay = max_delay.total_seconds()
        self._buffer: list[T] = []
        # When the oldest buffered value came, so its append starts at most max_delay later.
        self._first_at = 0.0
        self._timer: asyncio.TimerHandle | None = None
        self._writing: asyncio.Task[None] | None = None
        self._published = 0
        # How many values an append took, written or not.
        self._settled = 0
        self._error: BaseException | None = None
        self.unwritten = 0
        """How many published values a failed append left unwritten."""

    def publish(self, value: T) -> None:
        """Add ``value`` to the buffer, and return at once."""
        if self._error is not None:
            raise self._error
        if not self._buffer:
            self._first_at = asyncio.get_running_loop().time()
        self._buffer.append(value)
        self._published += 1
        self._schedule()

    async def flush(self) -> None:
        """Write every value published before the call."""
        target = self._published
        while self._settled < target:
            if self._writing is None:
                self._start()
            assert self._writing is not None
            # A cancel of the caller leaves the append to finish, since it keeps its place in
            # the order.
            await asyncio.shield(self._writing)
        if self._error is not None:
            raise self._error

    def _schedule(self) -> None:
        if self._writing is not None or not self._buffer:
            return
        if len(self._buffer) >= MAX_APPEND_RECORDS:
            self._start()
        elif self._timer is None:
            self._timer = asyncio.get_running_loop().call_at(
                self._first_at + self._max_delay, self._start
            )

    def _start(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._writing is not None or not self._buffer:
            return
        batch = self._buffer[:MAX_APPEND_RECORDS]
        del self._buffer[:MAX_APPEND_RECORDS]
        self._writing = asyncio.get_running_loop().create_task(self._write(batch))

    async def _write(self, batch: list[T]) -> None:
        try:
            if self._error is None:
                await self._append(*batch)
            else:
                self.unwritten += len(batch)
        except Exception as err:
            self.unwritten += len(batch)
            self._error = err
        finally:
            self._settled += len(batch)
            self._writing = None
            # The values published during the append wait for their own delay.
            self._schedule()

    async def __aenter__(self) -> AppendBuffer[T]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # An error of the append does not hide an error of the block.
        try:
            await self.flush()
        except Exception:
            if exc is None:
                raise


# ---------------------------------------------------------------------------
# Superseded attempts
# ---------------------------------------------------------------------------


def producer_epoch(producer: Producer | None) -> tuple[tuple[str, str], tuple[int, ...]] | None:
    """The group and the epoch of a Producer, or ``None`` for one that nothing supersedes.

    One Producer id is one unit of work, and its epoch is one attempt at it. A Local Activity
    compares by its Workflow Task's started event id, attempt and started time, then its own
    attempt. A Workflow Task retry runs the same append again, so its Records supersede the
    failed Task's. An Activity compares by its attempt's scheduled time, then its attempt. A
    client has no epoch, and a Workflow Producer only closes, so neither is superseded.
    """
    if producer is None:
        return None
    kind = producer.kind
    if isinstance(kind, ProducerKindLocalActivity):
        task = kind.value.epoch.task
        epoch = (
            _n(task.started_event_id),
            _n(task.attempt),
            *_time(task.started_time),
            _n(kind.value.epoch.attempt),
        )
        return ("local-activity", producer.id), epoch
    if isinstance(kind, (ProducerKindWorkflowActivity, ProducerKindStandaloneActivity)):
        activity = kind.value.epoch
        epoch = (*_time(activity.attempt_scheduled_time), _n(activity.attempt))
        return ("activity", producer.id), epoch
    return None


def _time(value: Timestamp | None) -> tuple[int, int]:
    return (_n(value.seconds), _n(value.nanos)) if value is not None else (0, 0)


def _n(value: int | None) -> int:
    # A missing or negative field compares as zero, as the Store compares it.
    return max(value or 0, 0)


@dataclass
class _Latest(Generic[T]):
    epoch: tuple[int, ...]
    records: list[tuple[bytes, T]]


class Attempts(Generic[T]):
    """For each Producer id, the highest epoch seen and the Records of that epoch.

    ``see`` takes the Records in Stream order. A Record of a higher epoch than the highest one
    seen for its Producer supersedes the Records of the lower epoch, and ``see`` gives them, so a
    reader can withdraw them before it gives the Record. Each Record carries a ``note`` of the
    caller's, which comes back with it.
    """

    def __init__(self) -> None:
        self._latest: dict[tuple[str, str], _Latest[T]] = {}

    def see(self, producer: Producer | None, offset: bytes, note: T) -> list[tuple[bytes, T]]:
        """Take one Record, and give the Records that it supersedes, oldest first."""
        found = producer_epoch(producer)
        if found is None:
            return []
        group, epoch = found
        latest = self._latest.get(group)
        if latest is None or epoch > latest.epoch:
            self._latest[group] = _Latest(epoch, [(offset, note)])
            return latest.records if latest is not None else []
        if epoch == latest.epoch:
            latest.records.append((offset, note))
        # The Store fences a lower epoch after a higher one, so a Record of one is not expected.
        # It supersedes nothing.
        return []
