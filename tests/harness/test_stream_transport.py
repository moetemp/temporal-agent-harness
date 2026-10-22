# ABOUTME: Tests for the turn-event reader and the activity publisher, on a fake Stream.
# They pin what the reader adds to a Stream read: the Superseded marker a consumer can act on,
# the subtree filter, the end of the read, and the cursor format.

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest
from temporalio.api.streamservice.v1 import ProducerKind
from temporalio.contrib.streams import (
    ActivityStream,
    Closed,
    Record,
    StreamNotFoundError,
    Superseded,
    WorkflowStream,
)
from temporalio.contrib.streams.system import models

import temporal_agent_harness.harness.stream_transport as transport
from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AgentStream,
    AttemptSuperseded,
    ReplyDelta,
    TurnEnded,
    subagent_topic,
    subtree_prefix,
)
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner
from temporal_agent_harness.harness.stream_context import TurnStreamContext
from temporal_agent_harness.harness.stream_transport import (
    TurnCursorError,
    TurnStream,
    cursor_after,
    next_turn_cursor,
    parse_cursor,
    read_turn_events,
)


@dataclass
class _FakeTopic:
    name: str
    type: Any = None
    batches: list[tuple[Any, ...]] = field(default_factory=list)

    async def publish(self, *values: Any) -> int:
        self.batches.append(values)
        return len(self.batches)


class _FakeStream:
    """A Stream whose read gives scripted items, and which records what it was asked."""

    def __init__(self, items: list[Any]) -> None:
        self._items = items
        self.reads: list[dict[str, Any]] = []

    def topic(self, name: str, *, type: Any = None) -> _FakeTopic:
        return _FakeTopic(name, type)

    def subscribe(
        self,
        *topics: _FakeTopic,
        from_offset: int | None = None,
        prefixes: Sequence[str] = (),
        create_wait: timedelta | None = None,
        **_: Any,
    ) -> AsyncIterator[Any]:
        self.reads.append(
            {
                "topics": [(t.name, t.type) for t in topics],
                "prefixes": list(prefixes),
                "from": from_offset,
                "create_wait": create_wait,
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


async def test_a_closed_item_ends_the_read():
    end = Closed(offset=3, result=None, failure=None)
    out, _ = await _drain([_record(1, _event("root", "a")), end, _record(4, _event("root", "b"))])
    assert [r.offset for r in out] == [1]


async def test_a_read_of_a_stream_that_was_never_created_ends():
    # An agent that ended before it created its Stream has nothing to read. A read with no
    # create wait gives StreamNotFoundError at its first step.
    class _Missing(_FakeStream):
        def subscribe(self, *topics: _FakeTopic, **kw: Any) -> AsyncIterator[Any]:
            super().subscribe(*topics, **kw)

            async def items() -> AsyncIterator[Any]:
                raise StreamNotFoundError("no Stream", reason="stream_not_found")
                yield

            return items()

    located = TurnStream(_Missing([]), TURN_EVENTS_TOPIC)  # type: ignore[arg-type]
    assert [record async for record in read_turn_events(located)] == []


async def test_the_own_read_filters_to_the_agents_topic():
    _, stream = await _drain([], after="7")
    assert stream.reads == [
        {
            "topics": [(TURN_EVENTS_TOPIC, AgentEvent)],
            "prefixes": [],
            "from": 7,
            "create_wait": None,
        }
    ]


async def test_the_subtree_read_asks_for_the_agent_and_its_subagents_only():
    child = subagent_topic("root-aaaaaa")
    _, stream = await _drain([], topic=child, subtree=True)
    assert stream.reads == [
        {
            "topics": [(child, AgentEvent)],
            "prefixes": [f"{child}-"],
            "from": None,
            "create_wait": None,
        }
    ]


async def test_a_record_of_a_prefix_topic_decodes_to_an_agent_event():
    # A read has no type for a Topic that only a prefix names, so its values come as JSON.
    child = subagent_topic("root-aaaaaa")
    grandchild = subagent_topic("root-aaaaaa-bbbbbb")
    plain = _event("root-aaaaaa-bbbbbb", "deep").model_dump(mode="json")
    out, _ = await _drain(
        [_record(5, plain, topic=grandchild)],  # type: ignore[arg-type]
        topic=child,
        subtree=True,
    )
    assert [r.data for r in out] == [_event("root-aaaaaa-bbbbbb", "deep")]


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


def _stream_context(topic: str) -> tuple[TurnStreamContext, list[_FakeTopic]]:
    # The handle is real, so the context validates as it does in an activity input. Only its
    # Topic is fake.
    stream = ActivityStream(id="wf:run")
    opened: list[_FakeTopic] = []

    def fake_topic(name: str, *, type: Any = None) -> _FakeTopic:
        opened.append(_FakeTopic(name, type))
        return opened[-1]

    stream.topic = fake_topic  # type: ignore[method-assign]
    context = TurnStreamContext(
        turn_id="turn-1", turn_number=1, agent_id="abc", stream=stream, topic=topic
    )
    return context, opened


async def test_the_activity_publisher_flushes_the_tail_when_the_activity_fails():
    context, opened = _stream_context(subagent_topic("abc"))
    with pytest.raises(RuntimeError):
        async with AgentWorkflowRunner.publisher_from_activity(
            context, batch_interval=timedelta(hours=1)
        ) as publisher:
            publisher.publish(ReplyDelta(text="half"))
            raise RuntimeError("model call failed")
    [topic] = opened
    assert (topic.name, topic.type) == (subagent_topic("abc"), AgentEvent)
    assert [[e.event for e in batch] for batch in topic.batches] == [[ReplyDelta(text="half")]]


async def test_an_activity_write_goes_out_after_the_buffered_events():
    context, opened = _stream_context(TURN_EVENTS_TOPIC)
    async with AgentWorkflowRunner.publisher_from_activity(
        context, batch_interval=timedelta(hours=1)
    ) as publisher:
        publisher.publish(ReplyDelta(text="a"))
        await publisher.write(TurnEnded())
        # The write returns when it is written, with what was buffered before it.
        [topic] = opened
        assert [e.event for batch in topic.batches for e in batch] == [
            ReplyDelta(text="a"),
            TurnEnded(),
        ]


async def test_an_activity_publisher_needs_a_stream():
    context = TurnStreamContext(turn_id="turn-1", turn_number=1, agent_id="abc")
    with pytest.raises(RuntimeError, match="no Stream"):
        async with AgentWorkflowRunner.publisher_from_activity(context):
            pass



async def test_the_next_cursor_of_a_stream_not_written_yet_is_the_beginning(monkeypatch):
    # The root creates its Stream in its first Workflow Task, and get_offset() raises until
    # then. A client that sends the first message reads from the beginning.
    class _Unwritten(_FakeStream):
        async def get_offset(self) -> int:
            raise StreamNotFoundError("no Stream yet", reason="stream_not_found")

    async def fake_turn_stream(_client: Any, _workflow_id: str, **_: Any) -> TurnStream:
        return TurnStream(_Unwritten([]), TURN_EVENTS_TOPIC)  # type: ignore[arg-type]

    monkeypatch.setattr(transport, "turn_stream", fake_turn_stream)
    assert await next_turn_cursor(None, "wf") == ""  # type: ignore[arg-type]


def test_the_subagent_stream_handle_uses_the_proto_json_names():
    # A memo or a config outlives this SDK, so the handle is stored as the proto JSON of a
    # StreamRef, which every SDK reads.
    place = AgentStream(
        handle=WorkflowStream(
            id="wf:run:agent_events",
            workflow_id="wf",
            first_execution_run_id="run",
            backend="memory",
        ),
        topic=subagent_topic("abc"),
    )
    stored = place.model_dump(mode="json")
    assert stored["handle"] == {
        "id": "wf:run:agent_events",
        "workflowId": "wf",
        "firstExecutionRunId": "run",
        "backend": "memory",
    }
    assert AgentStream.model_validate(stored) == place
    # A handle with no owner, such as a client's Stream, keeps only its id.
    standalone = AgentStream(handle=WorkflowStream(id="solo"), topic=TURN_EVENTS_TOPIC)
    assert standalone.model_dump(mode="json")["handle"] == {"id": "solo"}
