# ABOUTME: Tests for the unified subagent event stream (harness/agent_follow.py), the client side
# that follows a root agent's stream and, through Link Records, every subagent stream it drives.
# The streams are written in the store with real OPEN and JOIN links, the way the agents write
# them, and then followed. The checker asserts the contract: each stream's own order, and each
# subagent turn inside its [subagent_message_sent ... subagent_reply_received] bracket. The order
# of events that no bracket relates is not part of the contract, except that the same store
# content always gives the same order.

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio
from temporalio import workflow
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.streams import (
    Cursor,
    LinkEvent,
    StreamLink,
    StreamProducer,
    StreamRef,
    StreamStorePlugin,
    stream_handle,
)
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_follow import (
    FollowedEvent,
    ResumePoint,
    follow_agent_events,
)
from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS,
    AgentEvent,
    AgentEventType,
    AgentMessageReply,
    AgentStateSnapshot,
    MessageDisposition,
    MessageHandlerEnd,
    SubagentMessageSent,
    SubagentReplyReceived,
    SubagentStreamUnavailable,
    ToolEndEvent,
    ToolStartEvent,
    TurnEnded,
    TurnStarted,
)
from temporal_agent_harness.harness.stream_transport import (
    DEFAULT_REDIS_URL,
    REDIS_URL_ENV,
    link_name,
    store_name,
)
from tests._streams import TEST_DEFAULT_STORE, workflow_environment

# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------


def _ev(agent_id: str, turn_number: int, payload: Any, *, turn_id: str | None = None) -> AgentEvent:
    return AgentEvent(
        agent_id=agent_id,
        turn_id=turn_id if turn_id is not None else f"{agent_id}-t{turn_number}",
        turn_number=turn_number,
        timestamp=0.0,
        event=payload,
    )


def _ts(agent_id: str, turn: int) -> AgentEvent:
    return _ev(agent_id, turn, TurnStarted())


def _reply(agent_id: str, turn: int) -> AgentEvent:
    return _ev(agent_id, turn, MessageHandlerEnd(output={"ok": True}))


def _te(agent_id: str, turn: int) -> AgentEvent:
    return _ev(agent_id, turn, TurnEnded())


def _tool(agent_id: str, turn: int, tid: str, *, start: bool) -> AgentEvent:
    payload = (
        ToolStartEvent(tool_id=tid, tool_name="x", tool_input={})
        if start
        else ToolEndEvent(tool_id=tid, tool_name="x", tool_output="done")
    )
    return _ev(agent_id, turn, payload)


def _state_snapshot(agent_id: str) -> AgentEvent:
    """What an agent publishes when it registers observable state in ``@workflow.init``."""
    return _ev(
        agent_id,
        0,
        AgentStateSnapshot(state_id="plan", version=0, value={"steps": []}),
        turn_id="",
    )


# ---------------------------------------------------------------------------
# Store, owners and the writer
# ---------------------------------------------------------------------------


@workflow.defn
class _Owner:
    """A stream's owner. The store asks Temporal about it, so it must exist."""

    def __init__(self) -> None:
        self._closed = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._closed)

    @workflow.signal
    def close(self) -> None:
        self._closed = True


def _store() -> StreamStorePlugin:
    """The suite's store, with no owner start grace, so a missing owner fails at once."""
    if store_name(TEST_DEFAULT_STORE) == "redis":
        from temporalio.contrib.streams.redis import RedisStreams

        return RedisStreams(
            os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL),
            key_prefix=f"test-follow-{uuid.uuid4().hex}",
            owner_start_grace=timedelta(0),
        )
    from temporalio.contrib.streams.memory import MemoryStreams

    return MemoryStreams(owner_start_grace=timedelta(0))


@dataclass
class _Streams:
    """Writes agent streams the way the agents write them, with real links."""

    client: Client
    task_queue: str
    prefix: str
    _producers: dict[str, StreamProducer[AgentEvent]] = field(default_factory=dict)
    # Each agent's events in the order written, which is each stream's own order.
    written: dict[str, list[AgentEvent]] = field(default_factory=dict)

    def wf(self, agent_id: str) -> str:
        return f"{self.prefix}-{agent_id}"

    async def start(self, *agent_ids: str) -> None:
        for agent_id in agent_ids:
            await self.client.start_workflow(
                _Owner.run, id=self.wf(agent_id), task_queue=self.task_queue
            )

    async def close(self, agent_id: str) -> None:
        await self.client.get_workflow_handle(self.wf(agent_id)).signal(_Owner.close)
        await self.client.get_workflow_handle(self.wf(agent_id)).result()

    def _producer(self, agent_id: str) -> StreamProducer[AgentEvent]:
        producer = self._producers.get(agent_id)
        if producer is None:
            handle = stream_handle(self.client, self.wf(agent_id))
            producer = handle.producer(topic=TURN_EVENTS, producer_id="script", attempt=1)
            self._producers[agent_id] = producer
        return producer

    async def emit(self, *events: AgentEvent) -> Cursor:
        """Append ``events``, all of one agent, and return the cursor of the last."""
        agent_id = events[0].agent_id
        self.written.setdefault(agent_id, []).extend(events)
        return await self._producer(agent_id).append(*events)

    async def send(
        self, parent: str, parent_turn: int, child: str, child_turn: int, after: Cursor | None
    ) -> None:
        """``subagent_message_sent`` and the ``OPEN`` after it, as the activity writes them."""
        await self.emit(
            _ev(
                parent,
                parent_turn,
                SubagentMessageSent(
                    subagent_id=child,
                    agent_key="k",
                    workflow_id=self.wf(child),
                    handler="f",
                    subagent_turn=child_turn,
                    after_cursor=after.token if after else "",
                ),
            )
        )
        await self.link(parent, child, child_turn, after)

    async def link(
        self, parent: str, child: str, child_turn: int, after: Cursor | None, *, wf: str = ""
    ) -> None:
        await self._producer(parent).link(
            StreamRef.for_workflow(wf or self.wf(child), topic=TURN_EVENTS),
            name=link_name(child, child_turn),
            after=after,
        )

    async def received(
        self, parent: str, parent_turn: int, child: str, child_turn: int, until: Cursor | None
    ) -> None:
        """The ``JOIN`` and ``subagent_reply_received`` after it, as the workflow writes them."""
        await self._producer(parent).join(name=link_name(child, child_turn), until=until)
        await self.emit(
            _ev(
                parent,
                parent_turn,
                SubagentReplyReceived(
                    subagent_id=child,
                    agent_key="k",
                    workflow_id=self.wf(child),
                    handler="f",
                    subagent_turn=child_turn,
                    outcome="ok",
                ),
            )
        )

    async def child_turn(self, child: str, turn: int, *middle: AgentEvent) -> Cursor:
        """One whole child turn. Returns the cursor of its ``turn_end``."""
        return await self.emit(_ts(child, turn), *middle, _reply(child, turn), _te(child, turn))

    def follow(self, root: str = "P", **kwargs: Any) -> AsyncIterator[FollowedEvent]:
        kwargs.setdefault("resume", ResumePoint())
        return follow_agent_events(self.client, self.wf(root), **kwargs)


@pytest_asyncio.fixture
async def streams() -> AsyncIterator[_Streams]:
    env = await workflow_environment(data_converter=pydantic_data_converter)
    client = await Client.connect(
        env.client.service_client.config.target_host,
        namespace=env.client.namespace,
        data_converter=pydantic_data_converter,
        plugins=[_store()],
    )
    task_queue = f"agent-follow-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[_Owner],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        try:
            yield _Streams(client, task_queue, f"follow-{uuid.uuid4().hex[:8]}")
        finally:
            await env.shutdown()


async def _take(
    events: AsyncIterator[FollowedEvent], count: int, timeout: float = 10.0
) -> list[FollowedEvent]:
    """The first ``count`` events, then the follow is closed."""
    out: list[FollowedEvent] = []
    try:
        async with asyncio.timeout(timeout):
            async for followed in events:
                out.append(followed)
                if len(out) == count:
                    break
    finally:
        await events.aclose()  # type: ignore[attr-defined]
    return out


async def _quiet(events: AsyncIterator[FollowedEvent], wait: float = 0.5) -> None:
    """Assert the follow gives nothing more within ``wait`` seconds."""
    try:
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(events), wait)  # type: ignore[call-overload]
    finally:
        await events.aclose()  # type: ignore[attr-defined]


def _all(streams: _Streams) -> int:
    return sum(len(events) for events in streams.written.values())


def _events(followed: list[FollowedEvent]) -> list[AgentEvent]:
    return [f.event for f in followed]


def _names(events: list[AgentEvent]) -> list[str]:
    """Each event as ``<agent>:<turn>:<type>``, for pinned orders."""
    return [f"{e.agent_id}:{e.turn_number}:{e.event.type.value}" for e in events]


def assert_valid(merged: list[AgentEvent], written: dict[str, list[AgentEvent]]) -> None:
    """Each stream's own order with nothing lost or repeated, and both brackets.

    For every ``subagent_reply_received(C, T)`` there is an earlier
    ``subagent_message_sent(C, T)``, and all of child C's turn-T events are between the two.
    """
    for agent_id, events in written.items():
        got = [m for m in merged if m.agent_id == agent_id]
        assert got == events, f"stream {agent_id}: own order or completeness violated"
    for i, m in enumerate(merged):
        if not isinstance(m.event, SubagentReplyReceived):
            continue
        child, t = m.event.subagent_id, m.event.subagent_turn
        opens = [
            j
            for j, x in enumerate(merged)
            if isinstance(x.event, SubagentMessageSent)
            and x.event.subagent_id == child
            and x.event.subagent_turn == t
        ]
        assert opens and opens[0] < i, f"reply_received({child},{t}) before its message_sent"
        for k, x in enumerate(merged):
            if x.agent_id == child and x.turn_number == t:
                assert opens[0] < k < i, f"child {child} turn {t} event outside its bracket"


# ---------------------------------------------------------------------------
# Order: brackets, recursion, concurrency
# ---------------------------------------------------------------------------


async def test_single_subagent_turn_is_nested_in_its_bracket(streams):
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end = await streams.child_turn("C", 1, _tool("C", 1, "t1", start=True))
    await streams.received("P", 1, "C", 1, end)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams)))
    assert_valid(merged, streams.written)


async def test_an_activity_retry_that_writes_the_same_open_shows_each_child_event_once(streams):
    # The dispatch Activity's first attempt writes the marker and its OPEN, and the child turn
    # starts. The retry writes both again. The follow pauses the child at the SUPERSEDED and
    # resumes it at the retry's OPEN, so no child event shows twice.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    marker = _ev(
        "P",
        1,
        SubagentMessageSent(
            subagent_id="C",
            agent_key="k",
            workflow_id=streams.wf("C"),
            handler="f",
            subagent_turn=1,
        ),
    )
    link = StreamLink(
        LinkEvent.OPEN,
        StreamRef.for_workflow(streams.wf("C"), topic=TURN_EVENTS),
        link_name("C", 1),
    )
    parent = stream_handle(streams.client, streams.wf("P"))
    for attempt in (1, 2):
        dispatch = parent.producer(topic=TURN_EVENTS, producer_id="dispatch", attempt=attempt)
        await dispatch.append(marker, link)
        if attempt == 1:
            await streams.emit(_ts("C", 1), _tool("C", 1, "t1", start=True))
    end = await streams.emit(_reply("C", 1), _te("C", 1))
    await streams.received("P", 1, "C", 1, end)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), 10))
    assert [e for e in merged if e.agent_id == "C"] == streams.written["C"]
    assert _names([e for e in merged if e.agent_id == "P"]) == [
        "P:1:turn_started",
        "P:1:subagent_message_sent",
        "P:1:attempt_superseded",
        "P:1:subagent_message_sent",
        "P:1:subagent_reply_received",
        "P:1:turn_end",
    ]
    assert not _markers(merged), "a paused child is not unavailable"


async def test_child_state_registered_before_any_turn_comes_after_the_first_open(streams):
    # Turn-0 events belong to no bracket. The first segment opens at the child's oldest record,
    # so they come right after the first OPEN, and the child's turn follows them.
    await streams.start("P", "C")
    await streams.emit(_state_snapshot("C"))
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end = await streams.child_turn("C", 1)
    await streams.received("P", 1, "C", 1, end)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams)))
    assert_valid(merged, streams.written)
    sent = next(i for i, e in enumerate(merged) if isinstance(e.event, SubagentMessageSent))
    assert merged[sent + 1].event.type == AgentEventType.STATE_SNAPSHOT


async def test_reused_subagent_two_turns_one_parent_turn(streams):
    # Each turn is its own segment, from the previous turn's turn_end to this turn's.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end1 = await streams.child_turn("C", 1)
    await streams.received("P", 1, "C", 1, end1)
    await streams.send("P", 1, "C", 2, end1)
    end2 = await streams.child_turn("C", 2)
    await streams.received("P", 1, "C", 2, end2)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams)))
    assert_valid(merged, streams.written)


async def test_each_turn_is_its_own_segment(streams):
    # An event the child publishes between two turns belongs to no bracket. It comes at the
    # start of the next turn's segment, so the parent's events between the turns don't wait on it.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end1 = await streams.child_turn("C", 1)
    await streams.received("P", 1, "C", 1, end1)
    await streams.emit(_te("P", 1))
    await streams.emit(_state_snapshot("C"))
    await streams.emit(_ts("P", 2))
    await streams.send("P", 2, "C", 2, end1)
    end2 = await streams.child_turn("C", 2)
    await streams.received("P", 2, "C", 2, end2)
    await streams.emit(_te("P", 2))

    merged = _events(await _take(streams.follow(), _all(streams)))
    assert_valid(merged, streams.written)
    second = [
        i
        for i, e in enumerate(merged)
        if isinstance(e.event, SubagentMessageSent) and e.event.subagent_turn == 2
    ][0]
    assert merged[second + 1].event.type == AgentEventType.STATE_SNAPSHOT


async def test_two_concurrent_subagents_overlapping_brackets(streams):
    await streams.start("P", "C", "D")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.send("P", 1, "D", 1, None)
    end_c = await streams.child_turn("C", 1, _tool("C", 1, "c", start=True))
    end_d = await streams.child_turn("D", 1, _tool("D", 1, "d", start=True))
    await streams.received("P", 1, "D", 1, end_d)
    await streams.received("P", 1, "C", 1, end_c)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams)))
    assert_valid(merged, streams.written)


async def test_nested_grandchild_recursion(streams):
    await streams.start("P", "C", "G")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.emit(_ts("C", 1))
    await streams.send("C", 1, "G", 1, None)
    end_g = await streams.child_turn("G", 1)
    await streams.received("C", 1, "G", 1, end_g)
    end_c = await streams.emit(_reply("C", 1), _te("C", 1))
    await streams.received("P", 1, "C", 1, end_c)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams)))
    assert_valid(merged, streams.written)


async def test_replay_is_deterministic(streams):
    # The same store content gives the same order every time, and it is this one: the root
    # first within each round, so C and D come where the follow read them.
    await streams.start("P", "C", "D")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.send("P", 1, "D", 1, None)
    end_c = await streams.child_turn("C", 1)
    end_d = await streams.child_turn("D", 1)
    await streams.received("P", 1, "C", 1, end_c)
    await streams.received("P", 1, "D", 1, end_d)
    await streams.emit(_te("P", 1))

    runs = [_events(await _take(streams.follow(), _all(streams))) for _ in range(3)]
    assert runs[0] == runs[1] == runs[2]
    assert_valid(runs[0], streams.written)
    assert _names(runs[0]) == [
        "P:1:turn_started",
        "P:1:subagent_message_sent",
        "P:1:subagent_message_sent",
        "C:1:turn_started",
        "C:1:message_handler_end",
        "C:1:turn_end",
        "D:1:turn_started",
        "D:1:message_handler_end",
        "D:1:turn_end",
        "P:1:subagent_reply_received",
        "P:1:subagent_reply_received",
        "P:1:turn_end",
    ]


# ---------------------------------------------------------------------------
# The send_message preamble and stop
# ---------------------------------------------------------------------------


async def test_send_message_skip_preamble_starts_at_target_turn_started(streams):
    # The prior turn's tail, and the subagent it drove, are dropped. The follow starts at the
    # submitted turn's turn_started.
    await streams.start("P", "C", "D")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end_c = await streams.child_turn("C", 1)
    await streams.received("P", 1, "C", 1, end_c)
    await streams.emit(_te("P", 1))
    await streams.emit(_ts("P", 2))
    await streams.send("P", 2, "D", 1, None)
    end_d = await streams.child_turn("D", 1)
    await streams.received("P", 2, "D", 1, end_d)
    await streams.emit(_te("P", 2))

    merged = _events(await _take(streams.follow(skip_until_turn_id="P-t2"), 7))
    assert merged[0].turn_id == "P-t2"
    assert merged[0].event.type == AgentEventType.TURN_STARTED
    assert {e.agent_id for e in merged} == {"P", "D"}
    assert_valid(merged, {"P": streams.written["P"][-4:], "D": streams.written["D"]})


async def test_send_message_reused_child_starts_after_its_cursor(streams):
    # The submitted turn re-uses C. Its segment opens after C's turn-1 turn_end, so C's first
    # turn is not shown again.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end1 = await streams.child_turn("C", 1)
    await streams.received("P", 1, "C", 1, end1)
    since = await streams.emit(_te("P", 1))
    await streams.emit(_ts("P", 2))
    await streams.send("P", 2, "C", 2, end1)
    end2 = await streams.child_turn("C", 2)
    await streams.received("P", 2, "C", 2, end2)
    await streams.emit(_te("P", 2))

    merged = _events(
        await _take(
            streams.follow(resume=ResumePoint(cursor=since.token), skip_until_turn_id="P-t2"), 7
        )
    )
    assert [e.turn_number for e in merged if e.agent_id == "C"] == [2, 2, 2]
    assert_valid(merged, {"P": streams.written["P"][-4:], "C": streams.written["C"][3:]})


async def test_stop_predicate_ends_the_stream_at_root_turn_end(streams):
    # send_message stops at the submitted turn's root turn_end, and not at a subagent's.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end = await streams.child_turn("C", 1)
    await streams.received("P", 1, "C", 1, end)
    await streams.emit(_te("P", 1), _ts("P", 2))

    reply = AgentMessageReply(
        turn_number=1, turn_id="P-t1", message_id="m", disposition=MessageDisposition.OPENED
    )
    client = AgentClient(streams.client, streams.wf("P"))
    turn = client._followed_turn(
        reply, since="", on_item=lambda item, _resume: item, timeout=10, max_depth=4, max_open=16
    )
    got = [item async for item in turn]
    assert all(isinstance(item, AgentEvent) for item in got)
    last = got[-1]
    assert isinstance(last, AgentEvent)
    assert last.agent_id == "P"
    assert last.event.type == AgentEventType.TURN_END
    assert len(got) == _all(streams) - 1


async def test_empty_root_stream_yields_nothing(streams):
    await streams.start("P")
    await streams.close("P")
    assert await _take(streams.follow(), 1) == []


async def test_unreadable_root_ends_without_raising(streams):
    # The root's owner never started. A failed root ends the stream, and a consumer recovers
    # with a new attach.
    assert await _take(streams.follow("missing"), 1) == []


# ---------------------------------------------------------------------------
# Resume points
# ---------------------------------------------------------------------------


def test_resume_point_round_trips():
    point = ResumePoint(cursor="memory:abc:7", seq=3, state="AAEC")
    text = point.encode()
    assert text == "3@memory:abc:7#AAEC"
    assert ResumePoint.decode(text) == point
    # The older form, with no follow state, still decodes, as the root cursor only.
    assert ResumePoint.decode("3@memory:abc:7") == ResumePoint(cursor="memory:abc:7", seq=3)
    assert ResumePoint().encode() == ""
    assert ResumePoint.decode("") == ResumePoint()


@pytest.mark.parametrize("text", ["7", "x@7", "redis:3@7", "@", "1@memory:a:1#not base64!"])
def test_malformed_resume_point_is_rejected(text):
    with pytest.raises(ValueError):
        ResumePoint.decode(text)


async def _two_turns(streams: _Streams) -> None:
    """Two root turns, the second one driving C."""
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1), _tool("P", 1, "a", start=True), _te("P", 1))
    await streams.emit(_ts("P", 2))
    await streams.send("P", 2, "C", 1, None)
    end = await streams.child_turn("C", 1, _tool("C", 1, "c", start=True))
    await streams.received("P", 2, "C", 1, end)
    await streams.emit(_te("P", 2))


async def test_resume_point_moves_at_batch_ends(streams):
    # The store content fits one batch. Every event but the last carries the start point, and
    # the last carries the point after the batch, from which nothing is left.
    await _two_turns(streams)
    followed = await _take(streams.follow(), _all(streams))
    assert all(f.resume == ResumePoint() for f in followed[:-1])
    last = followed[-1].resume
    assert last.cursor and last.state
    await _quiet(streams.follow(resume=last))


async def test_resume_after_a_point_streams_only_events_after_it(streams):
    await streams.start("P")
    first = await streams.emit(_ts("P", 1), _te("P", 1))
    await streams.emit(_ts("P", 2), _te("P", 2))

    merged = _events(await _take(streams.follow(resume=ResumePoint(cursor=first.token)), 2))
    assert _names(merged) == ["P:2:turn_started", "P:2:turn_end"]


async def test_resume_mid_turn_of_the_root_streams_the_rest_of_that_turn(streams):
    await streams.start("P")
    started = await streams.emit(_ts("P", 1))
    await streams.emit(_tool("P", 1, "a", start=True), _te("P", 1))

    merged = _events(await _take(streams.follow(resume=ResumePoint(cursor=started.token)), 2))
    assert _names(merged) == ["P:1:tool_start", "P:1:turn_end"]


async def _root_cursor(streams: _Streams, index: int) -> Cursor:
    """The cursor of the root's record at ``index``, link records counted."""
    records = stream_handle(streams.client, streams.wf("P")).read(topic=TURN_EVENTS)
    try:
        at = 0
        async for record in records:
            if at == index:
                return record.cursor
            at += 1
    finally:
        await records.aclose()
    raise AssertionError(f"the root has no record {index}")


async def test_resume_includes_subagent_dispatched_at_or_after_the_point(streams):
    await _two_turns(streams)
    assert streams.written["P"][2].event.type == AgentEventType.TURN_END

    after = await _root_cursor(streams, 2)
    merged = _events(await _take(streams.follow(resume=ResumePoint(cursor=after.token)), 8))
    assert {e.agent_id for e in merged} == {"P", "C"}
    assert_valid(merged, {"P": streams.written["P"][3:], "C": streams.written["C"]})


async def test_resume_inside_subagent_turn_gets_the_rest_of_that_turn(streams):
    # A point from inside C's turn holds C's cursor, so the resumed stream gets the rest of C's
    # turn, and then the parent's reply.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.emit(_ts("C", 1), _tool("C", 1, "c", start=True))

    head = await _take(streams.follow(), 4)
    point = head[-1].resume
    assert point.state

    await streams.emit(_tool("C", 1, "c", start=False), _reply("C", 1))
    end = await streams.emit(_te("C", 1))
    await streams.received("P", 1, "C", 1, end)
    await streams.emit(_te("P", 1))

    rest = _events(await _take(streams.follow(resume=point), 5))
    assert _names(rest) == [
        "C:1:tool_end",
        "C:1:message_handler_end",
        "C:1:turn_end",
        "P:1:subagent_reply_received",
        "P:1:turn_end",
    ]


async def test_resume_inside_subagent_turn_from_an_old_point_omits_that_subagent(streams):
    # A point in the older seq@cursor form holds the root only. The subagent whose turn opened
    # before it is not followed, and the parent's reply still flows.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    end = await streams.child_turn("C", 1)
    await streams.received("P", 1, "C", 1, end)
    await streams.emit(_te("P", 1))
    # The third root record is the OPEN.
    old = ResumePoint.decode(f"2@{(await _root_cursor(streams, 2)).token}")

    merged = _events(await _take(streams.follow(resume=old), 2))
    assert _names(merged) == ["P:1:subagent_reply_received", "P:1:turn_end"]


# ---------------------------------------------------------------------------
# Subagent streams that can't be read or followed
# ---------------------------------------------------------------------------


def _markers(merged: list[AgentEvent]) -> list[SubagentStreamUnavailable]:
    return [e.event for e in merged if isinstance(e.event, SubagentStreamUnavailable)]


async def test_unreadable_child_gives_a_marker_and_the_parent_flows(streams):
    # The child's owner never started. Its OPEN gives UNAVAILABLE, and the JOIN does not wait.
    await streams.start("P")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.received("P", 1, "C", 1, Cursor("memory:none:1"))
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams) + 1))
    assert _names([e for e in merged if e.agent_id == "P"]) == _names(streams.written["P"])
    (marker,) = _markers(merged)
    assert marker.subagent_id == "C"
    assert marker.workflow_id == streams.wf("C")
    assert "unavailable" in marker.reason
    unavailable = next(e for e in merged if isinstance(e.event, SubagentStreamUnavailable))
    assert unavailable.agent_id == "C"


async def test_dead_child_releases_the_join_and_the_parent_completes(streams):
    # One child is readable, the other is not. The readable one keeps its bracket. The JOIN of
    # the other one does not hold the parent.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.send("P", 1, "D", 1, None)
    end_c = await streams.child_turn("C", 1)
    await streams.received("P", 1, "D", 1, Cursor("memory:none:1"))
    await streams.received("P", 1, "C", 1, end_c)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams) + 1))
    assert merged[-1].event.type == AgentEventType.TURN_END
    assert [m.subagent_id for m in _markers(merged)] == ["D"]
    assert_valid([e for e in merged if e.agent_id != "D"], streams.written)


async def test_a_child_whose_cursor_the_store_refuses_drops_only_that_child(streams):
    await streams.start("P", "C", "D")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, Cursor("foreign:0:0"))
    await streams.send("P", 1, "D", 1, None)
    await streams.child_turn("C", 1)
    end_d = await streams.child_turn("D", 1)
    await streams.received("P", 1, "D", 1, end_d)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams) - 3 + 1))
    assert [m.subagent_id for m in _markers(merged)] == ["C"]
    assert not [e for e in merged if e.agent_id == "C" and e.turn_number]
    assert_valid(
        [e for e in merged if e.agent_id != "C"],
        {"P": streams.written["P"], "D": streams.written["D"]},
    )


async def test_child_that_closes_without_turn_end_ends_its_segment(streams):
    # The parent waits for the child's end. The child's workflow closes with its turn open, so
    # the segment ends and the parent's reply flows.
    await streams.start("P", "C")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.emit(_ts("C", 1))
    await streams.close("C")
    await streams.received("P", 1, "C", 1, None)
    await streams.emit(_te("P", 1))

    merged = _events(await _take(streams.follow(), _all(streams)))
    assert_valid(merged, streams.written)
    assert merged[-1].event.type == AgentEventType.TURN_END


async def test_a_subagent_past_max_depth_gives_a_marker(streams):
    await streams.start("P", "C", "G")
    await streams.emit(_ts("P", 1))
    await streams.send("P", 1, "C", 1, None)
    await streams.emit(_ts("C", 1))
    await streams.send("C", 1, "G", 1, None)
    end_g = await streams.child_turn("G", 1)
    await streams.received("C", 1, "G", 1, end_g)
    end_c = await streams.emit(_reply("C", 1), _te("C", 1))
    await streams.received("P", 1, "C", 1, end_c)
    await streams.emit(_te("P", 1))

    count = _all(streams) - len(streams.written["G"]) + 1
    merged = _events(await _take(streams.follow(max_depth=1), count))
    (marker,) = _markers(merged)
    assert marker.subagent_id == "G"
    assert marker.reason.endswith("depth")
    assert_valid(
        [e for e in merged if e.agent_id != "G"],
        {"P": streams.written["P"], "C": streams.written["C"]},
    )
