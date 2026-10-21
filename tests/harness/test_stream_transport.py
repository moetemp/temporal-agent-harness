# ABOUTME: Tests for the turn-event reader and the activity publisher, on a fake Stream.
# They pin what the reader adds to a Stream read: the Superseded marker a consumer can act on,
# the subtree filter, the end of the read, and the cursor format.

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any

import pytest
from temporalio.api.streamservice.v1 import ProducerKind
from temporalio.contrib.streams import (
    Closed,
    Idle,
    Record,
    StreamNotFoundError,
    StreamRef,
    Superseded,
    Topic,
)
from temporalio.contrib.streams.system import models

import temporal_agent_harness.harness.stream_transport as transport
from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AgentStream,
    AttemptSuperseded,
    ReplyDelta,
    subagent_topic,
    subtree_prefix,
)
from temporal_agent_harness.harness.stream_transport import (
    TurnCursorError,
    TurnStream,
    cursor_after,
    next_turn_cursor,
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
        topic_prefixes: Sequence[str] = (),
        from_offset: int | None = None,
        type: Any = None,
        **_: Any,
    ) -> AsyncIterator[Any]:
        self.reads.append(
            {
                "topics": [t.name for t in topics] if topics else None,
                "prefixes": list(topic_prefixes),
                "from": from_offset,
                "type": type,
            }
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
    located = TurnStream(stream, topic)  # type: ignore[arg-type]
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
    assert stream.reads == [
        {"topics": [TURN_EVENTS_TOPIC], "prefixes": [], "from": 7, "type": AgentEvent}
    ]


async def test_the_subtree_read_asks_for_the_agent_and_its_subagents_only():
    child = subagent_topic("root-aaaaaa")
    _, stream = await _drain([], topic=child, subtree=True)
    assert stream.reads == [
        {"topics": [child], "prefixes": [f"{child}-"], "from": None, "type": AgentEvent}
    ]


def test_subtree_prefix():
    root = subtree_prefix(TURN_EVENTS_TOPIC)
    child = subagent_topic("abc")
    assert subagent_topic("abc").startswith(root)
    assert subagent_topic("abc-def").startswith(subtree_prefix(child))
    assert not subagent_topic("abcdef").startswith(subtree_prefix(child))
    assert not TURN_EVENTS_TOPIC.startswith(subtree_prefix(child))


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



async def test_the_next_cursor_of_a_stream_not_written_yet_is_the_beginning(monkeypatch):
    # A Workflow's default Stream does not exist until its first write, and get_offset()
    # raises until then. A client that sends the first message reads from the beginning.
    class _Unwritten(_FakeStream):
        async def get_offset(self) -> int:
            raise StreamNotFoundError("no Stream yet", reason="stream_not_found")

    async def fake_turn_stream(_client: Any, _workflow_id: str, **_: Any) -> TurnStream:
        return TurnStream(_Unwritten([]), TURN_EVENTS_TOPIC)  # type: ignore[arg-type]

    monkeypatch.setattr(transport, "turn_stream", fake_turn_stream)
    assert await next_turn_cursor(None, "wf") == ""  # type: ignore[arg-type]


def test_the_subagent_stream_ref_uses_the_proto_json_names():
    # A memo or a config outlives this SDK, so the ref is stored as the proto JSON that every
    # SDK reads. A ref stored with snake case names still decodes.
    place = AgentStream(
        ref=StreamRef("wf:run", workflow_id="wf", first_execution_run_id="run"),
        topic=subagent_topic("abc"),
    )
    stored = place.model_dump(mode="json")
    assert stored["ref"] == {"id": "wf:run", "workflowId": "wf", "firstExecutionRunId": "run"}
    assert AgentStream.model_validate(stored) == place
    old = {"ref": {"id": "wf:run", "workflow_id": "wf", "first_execution_run_id": "run"}}
    assert AgentStream.model_validate({**old, "topic": place.topic}) == place
    # A ref with no owner, such as a standalone Stream's, keeps only its id.
    standalone = AgentStream(ref=StreamRef("solo"), topic=TURN_EVENTS_TOPIC)
    assert standalone.model_dump(mode="json")["ref"] == {"id": "solo"}
