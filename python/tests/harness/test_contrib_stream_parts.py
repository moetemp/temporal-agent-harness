"""The parts of ``contrib_stream`` that the SDK's Streams API leaves to the harness, and the
Stream config that the harness builds from the environment.

They need no server. A released SDK has no Streams API, so the tests skip there.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

# A released SDK has no Streams API. The tests still collect there and skip, since a module
# with no test fails its Bazel target.
try:
    from temporalio.nexus.system.stream_service import (
        ActivityEpoch,
        ActivityProducer,
        ClientProducer,
        LocalActivityEpoch,
        LocalActivityProducer,
        Producer,
        ProducerKindClient,
        ProducerKindLocalActivity,
        ProducerKindWorkflowActivity,
        StreamRef,
        Timestamp,
        WorkflowTaskEpoch,
    )

    from temporal_agent_harness.harness._contrib_stream_parts import (
        MAX_APPEND_RECORDS,
        AppendBuffer,
        Attempts,
        ref_from_data,
        ref_to_data,
    )
except ImportError:
    pytestmark = pytest.mark.skip(reason="needs the SDK's Streams API")


class _Appends:
    """An append that keeps each batch, and fails the calls that ``fail`` names."""

    def __init__(self, *, delay: float = 0.0, fail: set[int] | None = None) -> None:
        self.batches: list[list[Any]] = []
        self._delay = delay
        self._fail = fail or set()
        self._in_flight = 0
        self.most_in_flight = 0

    async def __call__(self, *values: Any) -> None:
        self._in_flight += 1
        self.most_in_flight = max(self.most_in_flight, self._in_flight)
        try:
            await asyncio.sleep(self._delay)
            if len(self.batches) in self._fail:
                self.batches.append([])
                raise RuntimeError("the Store refused the append")
            self.batches.append(list(values))
        finally:
            self._in_flight -= 1


async def test_a_burst_keeps_its_order_with_one_append_in_flight():
    appends = _Appends(delay=0.01)
    async with AppendBuffer[int](appends, max_delay=timedelta(milliseconds=5)) as buffer:
        for value in range(MAX_APPEND_RECORDS * 2 + 10):
            buffer.publish(value)
        await asyncio.sleep(0.005)
        # Values published during an append wait for the next one.
        for value in range(MAX_APPEND_RECORDS * 2 + 10, MAX_APPEND_RECORDS * 2 + 20):
            buffer.publish(value)

    written = [value for batch in appends.batches for value in batch]
    assert written == list(range(MAX_APPEND_RECORDS * 2 + 20))
    assert all(len(batch) <= MAX_APPEND_RECORDS for batch in appends.batches)
    assert appends.most_in_flight == 1


async def test_an_append_starts_at_most_max_delay_after_the_first_value():
    appends = _Appends()
    buffer = AppendBuffer[str](appends, max_delay=timedelta(milliseconds=20))
    buffer.publish("a")
    buffer.publish("b")
    assert appends.batches == []

    await asyncio.sleep(0.2)

    assert appends.batches == [["a", "b"]]


async def test_leaving_a_failed_block_writes_the_tail():
    appends = _Appends()
    with pytest.raises(ValueError, match="the Activity failed"):
        async with AppendBuffer[str](appends, max_delay=timedelta(seconds=60)) as buffer:
            buffer.publish("half")
            buffer.publish("an answer")
            raise ValueError("the Activity failed")

    assert appends.batches == [["half", "an answer"]]


async def test_a_refused_append_stops_the_writes_after_it():
    appends = _Appends(fail={0})
    buffer = AppendBuffer[str](appends, max_delay=timedelta(milliseconds=1))
    buffer.publish("refused")
    with pytest.raises(RuntimeError, match="refused"):
        await buffer.flush()

    # A reader sees no gap, so nothing after the refused append is written.
    with pytest.raises(RuntimeError, match="refused"):
        buffer.publish("later")
    assert appends.batches == [[]] and buffer.unwritten == 1


async def test_leaving_a_block_raises_the_error_of_its_tail():
    appends = _Appends(fail={0})
    with pytest.raises(RuntimeError, match="refused"):
        async with AppendBuffer[str](appends, max_delay=timedelta(seconds=60)) as buffer:
            buffer.publish("refused")


def _local_activity(task_attempt: int, *, attempt: int = 1, event_id: int = 7) -> Producer:
    epoch = LocalActivityEpoch(
        task=WorkflowTaskEpoch(
            started_event_id=event_id, attempt=task_attempt, started_time=Timestamp(100, 0)
        ),
        attempt=attempt,
    )
    return Producer(
        id="la:run1:3",
        kind=ProducerKindLocalActivity(LocalActivityProducer(run_id="run1", epoch=epoch)),
    )


def _activity(attempt: int, scheduled: int, *, producer_id: str = "act:run1:5:9") -> Producer:
    epoch = ActivityEpoch(attempt_scheduled_time=Timestamp(scheduled, 0), attempt=attempt)
    return Producer(
        id=producer_id,
        kind=ProducerKindWorkflowActivity(ActivityProducer(run_id="run1", epoch=epoch)),
    )


def test_a_retried_workflow_task_supersedes_the_records_of_the_failed_one():
    attempts: Attempts[str] = Attempts()
    assert attempts.see(_local_activity(1), b"\x01", "first") == []
    assert attempts.see(_local_activity(1), b"\x02", "second") == []

    # The retried Workflow Task runs the same append, as the same Producer with a higher epoch.
    assert attempts.see(_local_activity(2), b"\x05", "retry") == [
        (b"\x01", "first"),
        (b"\x02", "second"),
    ]
    assert attempts.see(_local_activity(2), b"\x06", "retry, more") == []
    # A Local Activity retry in the same Workflow Task is a higher epoch too.
    assert attempts.see(_local_activity(2, attempt=2), b"\x07", "again") == [
        (b"\x05", "retry"),
        (b"\x06", "retry, more"),
    ]


def test_a_retried_activity_supersedes_the_records_of_the_failed_attempt():
    attempts: Attempts[str] = Attempts()
    assert attempts.see(_activity(1, 100), b"\x01", "half") == []
    assert attempts.see(_activity(1, 100, producer_id="act:run1:6:9"), b"\x02", "other") == []

    assert attempts.see(_activity(2, 103), b"\x03", "whole") == [(b"\x01", "half")]
    # Another Producer id keeps its own Records.
    assert attempts.see(_activity(2, 104, producer_id="act:run1:6:9"), b"\x04", "x") == [
        (b"\x02", "other")
    ]


def test_a_client_and_a_lower_epoch_supersede_nothing():
    attempts: Attempts[str] = Attempts()
    client = Producer(id="client:1", kind=ProducerKindClient(ClientProducer()))
    assert attempts.see(client, b"\x01", "a") == []
    assert attempts.see(client, b"\x02", "b") == []
    assert attempts.see(None, b"\x03", "c") == []

    assert attempts.see(_activity(2, 103), b"\x04", "retry") == []
    assert attempts.see(_activity(1, 100), b"\x05", "fenced") == []
    assert attempts.see(_activity(3, 106), b"\x06", "third") == [(b"\x04", "retry")]


def test_a_ref_goes_through_a_plain_dict():
    ref: StreamRef[Any] = StreamRef(id="wf:run:agent_events", store="redis")
    data = ref_to_data(ref)

    assert data == {"id": "wf:run:agent_events", "store": "redis"}
    assert ref_from_data(data) == ref
    assert ref_from_data({"id": "x"}) == StreamRef(id="x", store=None)


def test_a_cursor_is_hex_and_keeps_its_grammar():
    from temporal_agent_harness.harness import contrib_stream

    # ``str()`` of the offset of the older SDK was its hex, so a cursor that it gave still parses.
    position = contrib_stream._position("1a1d372cadf00002.s.t1a1d372cadf00000")
    assert position == contrib_stream._Position(
        bytes.fromhex("1a1d372cadf00002"), True, bytes.fromhex("1a1d372cadf00000")
    )
    assert position.encode() == "1a1d372cadf00002.s.t1a1d372cadf00000"
    assert contrib_stream.parse_cursor("") is None
    assert contrib_stream._position("00ff.t").turn_start == b""
    for bad in ("xyz", "abc", "0001.t0002"):
        with pytest.raises(contrib_stream.TurnCursorError):
            contrib_stream.parse_cursor(bad)


def test_the_config_from_the_environment_holds_a_run_through_a_store_outage(monkeypatch):
    from temporal_agent_harness.harness import contrib_stream

    monkeypatch.setenv("STREAMS_STORE", "memory")
    config = contrib_stream.stream_config_from_env()

    assert config is not None and [store.name for store in config.stores] == ["memory"]
    # Each Stream call of Workflow code retries until the Store is back.
    options = config.workflow_call_options
    assert options.retry_policy.maximum_attempts == 0
    assert options.start_to_close_timeout == timedelta(seconds=60)
    # Every client of the process gets the one memory Store.
    again = contrib_stream.stream_config_from_env()
    assert again is not None and again.stores[0] is config.stores[0]

    monkeypatch.delenv("STREAMS_STORE")
    assert contrib_stream.stream_config_from_env() is None
