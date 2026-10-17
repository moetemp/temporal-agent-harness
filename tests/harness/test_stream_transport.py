# ABOUTME: Tests for the batched publisher an activity streams through, on a fake producer.
# They pin what leaving the context guarantees: every queued value reaches the producer.

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest

from temporalio.streams import Cursor, RecordKind, StreamRecord, Supersession

from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS,
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AttemptSuperseded,
    ReplyDelta,
)
from temporal_agent_harness.harness.stream_transport import (
    ActivityPublisher,
    _turn_records,
)


class _SlowProducer:
    """A producer whose append suspends a few times before landing, like a real RPC."""

    def __init__(self) -> None:
        self.appended: list[list[Any]] = []
        self.entered = asyncio.Event()

    async def append(self, *values: Any) -> None:
        self.entered.set()
        for _ in range(3):
            await asyncio.sleep(0)
        self.appended.append(list(values))


async def test_exit_lets_the_batch_in_flight_land():
    producer = _SlowProducer()
    async with ActivityPublisher(producer, batch_interval=timedelta(0)) as publisher:
        publisher.publish("a")
        # The flusher is inside append with ["a"] when the context closes.
        await producer.entered.wait()
    assert producer.appended == [["a"]]


async def test_exit_sends_the_tail_without_waiting_for_the_interval():
    producer = _SlowProducer()
    async with ActivityPublisher(producer, batch_interval=timedelta(hours=1)) as publisher:
        publisher.publish("a")
        publisher.publish("b")
    assert producer.appended == [["a", "b"]]


async def test_publish_after_exit_is_refused():
    producer = _SlowProducer()
    async with ActivityPublisher(producer, batch_interval=timedelta(hours=1)) as publisher:
        pass
    with pytest.raises(RuntimeError):
        publisher.publish("late")
    assert producer.appended == []


def test_turn_events_definition_names_the_wire_topic():
    """The typed definition and the wire string are one topic, decoding to the envelope."""
    assert TURN_EVENTS.name == TURN_EVENTS_TOPIC
    assert TURN_EVENTS.result_type is AgentEvent


def _event(agent_id: str, turn_id: str, text: str) -> AgentEvent:
    return AgentEvent(
        agent_id=agent_id,
        turn_id=turn_id,
        turn_number=1,
        timestamp=0.0,
        event=ReplyDelta(text=text),
    )


def _record(kind: RecordKind, token: str, **kw: Any) -> StreamRecord[Any]:
    return StreamRecord(kind=kind, cursor=Cursor(token), topic=TURN_EVENTS_TOPIC, **kw)


async def _drain(records: list[StreamRecord[Any]]) -> list[StreamRecord[Any]]:
    async def source():
        for record in records:
            yield record

    return [record async for record in _turn_records(source())]


async def test_a_supersession_reaches_the_consumer_naming_the_turn_it_retires():
    # An activity that streams half an answer and then fails leaves those records
    # behind, and its retry writes different words. Dropped here, the consumer
    # renders both halves and shows the partial answer twice.
    first = _event("root", "turn-1", "half an ")
    out = await _drain(
        [
            _record(RecordKind.DATA, "p:1", value=first, producer_id="act", attempt=1),
            _record(
                RecordKind.SUPERSEDED,
                "p:1",
                supersession=Supersession("act", 1, 2),
            ),
            _record(
                RecordKind.DATA,
                "p:2",
                value=_event("root", "turn-1", "the whole answer"),
                producer_id="act",
                attempt=2,
            ),
        ]
    )
    assert [r.kind for r in out] == [
        RecordKind.DATA,
        RecordKind.SUPERSEDED,
        RecordKind.DATA,
    ]
    marker = out[1].value
    assert isinstance(marker.event, AttemptSuperseded)
    assert marker.event.superseded_attempt == 1 and marker.event.attempt == 2
    assert marker.event.producer_id == "act"
    # Stamped with the turn the retired records belonged to, which is how a
    # consumer knows what to drop.
    assert (marker.agent_id, marker.turn_id) == ("root", "turn-1")


async def test_a_supersession_before_any_record_retires_nothing_and_is_dropped():
    # Resuming past the earlier attempt means the consumer holds none of its
    # records, so there is no turn to name and nothing to retract.
    out = await _drain(
        [
            _record(
                RecordKind.SUPERSEDED,
                "p:1",
                supersession=Supersession("act", 1, 2),
            ),
            _record(
                RecordKind.DATA,
                "p:2",
                value=_event("root", "turn-1", "answer"),
                producer_id="act",
                attempt=2,
            ),
        ]
    )
    assert [r.kind for r in out] == [RecordKind.DATA]


async def test_a_finish_marker_is_not_a_turn_event():
    out = await _drain(
        [
            _record(RecordKind.FINISH, "p:1", producer_id="act"),
            _record(
                RecordKind.DATA,
                "p:2",
                value=_event("root", "turn-1", "answer"),
            ),
        ]
    )
    assert [r.kind for r in out] == [RecordKind.DATA]
