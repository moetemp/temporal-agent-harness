# ABOUTME: Tests for the turn-event reader and the activity publisher, on a fake Stream.
# They pin what the reader adds to a Stream read: the Superseded marker a consumer can act on,
# the subtree filter, the end of the read, and the cursor format.

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from types import SimpleNamespace
from typing import Any

import pytest
from temporalio.api.streamservice.v1 import ProducerKind
from temporalio.client import WorkflowExecutionStatus
from temporalio.contrib.streams import (
    Closed,
    Idle,
    Record,
    StreamNotFoundError,
    Superseded,
    Topic,
)
from temporalio.contrib.streams.system import models

import temporal_agent_harness.harness.stream_transport as transport
from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AttemptSuperseded,
    ReplyDelta,
    in_subtree,
    subagent_topic,
)
from temporal_agent_harness.harness.stream_transport import (
    TurnCursorError,
    TurnStream,
    cursor_after,
    parse_cursor,
    publisher_for_activity,
    read_turn_events,
)


class _FakeStream:
    """A Stream whose read gives scripted items, and which records what it was asked."""

    def __init__(self, items: list[Any]) -> None:
        self._items = items
        self.reads: list[dict[str, Any]] = []
        self.published: list[tuple[str, Any]] = []
        self.flushed = 0

    def topic(self, name: str, *, type: Any = Any) -> Topic[Any]:
        return Topic(self, name, type)  # type: ignore[arg-type]

    def _publish(self, topic: str, value: Any, force_flush: bool) -> None:
        self.published.append((topic, value))

    async def flush(self) -> None:
        self.flushed += 1

    def subscribe(
        self,
        *,
        topics: Sequence[Topic[Any]] | None = None,
        from_offset: int | None = None,
        **_: Any,
    ) -> AsyncIterator[Any]:
        self.reads.append(
            {"topics": [t.name for t in topics] if topics else None, "from": from_offset}
        )

        async def items() -> AsyncIterator[Any]:
            for item in self._items:
                yield item

        return items()


def _event(agent_id: str, text: str, turn_id: str = "turn-1") -> AgentEvent:
    return AgentEvent(
        agent_id=agent_id,
        turn_id=turn_id,
        turn_number=1,
        timestamp=0.0,
        event=ReplyDelta(text=text),
    )


def _producer(attempt: int) -> models.Producer:
    return models.Producer(
        kind=ProducerKind.PRODUCER_KIND_ACTIVITY,
        id="act",
        epoch=models.ProducerEpoch(attempt=attempt),
    )


def _record(offset: int, event: AgentEvent, *, topic: str = TURN_EVENTS_TOPIC, attempt: int = 1):
    return Record(topic=topic, data=event, offset=offset, producer=_producer(attempt))


def _superseded(offset: int, attempt: int) -> Superseded:
    return Superseded(
        offset=offset,
        supersession=models.Supersession(
            producer_kind=ProducerKind.PRODUCER_KIND_ACTIVITY,
            producer_id="act",
            epoch=models.ProducerEpoch(attempt=attempt),
        ),
    )


async def _drain(items: list[Any], *, topic: str = TURN_EVENTS_TOPIC, **kw: Any):
    stream = _FakeStream(items)
    located = TurnStream(stream, topic, root=None)  # type: ignore[arg-type]
    out = [record async for record in read_turn_events(located, **kw)]
    return out, stream


async def test_a_supersession_reaches_the_consumer_naming_the_turn_it_retires():
    # An activity that streams half an answer and then fails leaves those records
    # behind, and its retry writes different words. Dropped here, the consumer
    # renders both halves and shows the partial answer twice.
    out, _ = await _drain(
        [
            _record(10, _event("root", "half an ")),
            _superseded(11, attempt=2),
            _record(12, _event("root", "the whole answer"), attempt=2),
        ]
    )
    assert [r.offset for r in out] == [10, 11, 12]
    marker = out[1].data
    assert isinstance(marker.event, AttemptSuperseded)
    assert marker.event.superseded_attempt == 1 and marker.event.attempt == 2
    assert marker.event.producer_id == "act"
    # Stamped with the turn the withdrawn records belonged to, which is how a
    # consumer knows what to drop.
    assert (marker.agent_id, marker.turn_id) == ("root", "turn-1")


async def test_a_supersession_before_any_record_retires_nothing_and_is_dropped():
    # Resuming past the earlier attempt means the consumer holds none of its
    # records, so there is no turn to name and nothing to retract.
    out, _ = await _drain(
        [_superseded(11, attempt=2), _record(12, _event("root", "answer"), attempt=2)]
    )
    assert [r.offset for r in out] == [12]


@pytest.mark.parametrize("end", [Idle(offset=3), Closed(offset=3, result=None, failure=None)])
async def test_an_idle_or_closed_item_ends_the_read(end: Any):
    out, _ = await _drain([_record(1, _event("root", "a")), end, _record(4, _event("root", "b"))])
    assert [r.offset for r in out] == [1]


async def test_the_own_read_filters_to_the_agents_topic():
    _, stream = await _drain([], after="7")
    assert stream.reads == [{"topics": [TURN_EVENTS_TOPIC], "from": 7}]


async def test_the_subtree_read_keeps_the_agent_and_its_subagents_only():
    child = subagent_topic("root-aaaaaa")
    grandchild = subagent_topic("root-aaaaaa-bbbbbb")
    sibling = subagent_topic("root-cccccc")
    out, stream = await _drain(
        [
            _record(1, _event("root", "parent"), topic=TURN_EVENTS_TOPIC),
            _record(2, _event("root-aaaaaa", "child"), topic=child),
            _record(3, _event("root-aaaaaa-bbbbbb", "grandchild"), topic=grandchild),
            _record(4, _event("root-cccccc", "sibling"), topic=sibling),
        ],
        topic=child,
        subtree=True,
    )
    assert stream.reads == [{"topics": None, "from": None}]
    assert [r.data.agent_id for r in out] == ["root-aaaaaa", "root-aaaaaa-bbbbbb"]


async def test_a_read_of_every_topic_types_each_record():
    # A Topic named at run time has no type on the read, so it decodes to plain JSON.
    raw = _event("root-aaaaaa", "child").model_dump(mode="json")
    out, _ = await _drain(
        [Record(topic=subagent_topic("root-aaaaaa"), data=raw, offset=1, producer=None)],
        subtree=True,
    )
    assert isinstance(out[0].data, AgentEvent)


def test_in_subtree():
    root = TURN_EVENTS_TOPIC
    child = subagent_topic("abc")
    assert in_subtree(child, root)
    assert in_subtree(subagent_topic("abc-def"), child)
    assert not in_subtree(subagent_topic("abcdef"), child)
    assert not in_subtree(root, child)


def test_cursors():
    assert parse_cursor("") is None
    assert parse_cursor("0") is None
    assert parse_cursor("42") == 42
    assert cursor_after(_record(41, _event("root", "a"))) == "42"
    for bad in ("native:run-1:7", "-1", "x"):
        with pytest.raises(TurnCursorError):
            parse_cursor(bad)


async def test_the_activity_publisher_flushes_the_tail_when_the_activity_fails(monkeypatch):
    stream = _FakeStream([])
    opened: list[Any] = []

    def fake_activity_stream(ref: Any = None, **kw: Any) -> _FakeStream:
        opened.append((ref, kw))
        return stream

    monkeypatch.setattr(transport, "activity_stream", fake_activity_stream)
    with pytest.raises(RuntimeError):
        async with publisher_for_activity("turn_events/abc", stream_id="wf:run") as publisher:
            publisher.publish("half")
            raise RuntimeError("model call failed")
    assert stream.published == [("turn_events/abc", "half")]
    assert stream.flushed == 1
    assert opened[0][0].id == "wf:run"


class _MissingStream(_FakeStream):
    """A Stream that no agent wrote: every read fails with ``StreamNotFoundError``."""

    def subscribe(self, **kw: Any) -> AsyncIterator[Any]:
        self.reads.append(kw)

        async def items() -> AsyncIterator[Any]:
            raise StreamNotFoundError("no Stream", reason="stream_not_found")
            yield

        return items()


class _Root:
    def __init__(self, statuses: list[WorkflowExecutionStatus]) -> None:
        self._statuses = statuses

    async def describe(self) -> Any:
        return SimpleNamespace(status=self._statuses.pop(0))


async def test_a_read_of_a_stream_never_written_ends_with_the_run():
    # The read's own wait for the Stream does not look at the run, so a session that ended
    # before its first event would otherwise hold the reader forever.
    stream = _MissingStream([])
    root = _Root([WorkflowExecutionStatus.RUNNING, WorkflowExecutionStatus.COMPLETED])
    located = TurnStream(stream, TURN_EVENTS_TOPIC, root=root)  # type: ignore[arg-type]
    out = [record async for record in read_turn_events(located)]
    assert out == []
    assert len(stream.reads) == 2
