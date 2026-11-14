"""The failure paths of the two event-stream backends, against a real worker.

Each test runs on the lane's backend (``tests/_streams.py``): ``ContribStream()`` on the memory
and Redis lanes, and ``WorkflowStream()`` without one. A test of something only a
Stream has skips on Workflow Streams.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import uuid
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from temporalio import workflow
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHistory,
    WorkflowUpdateFailedError,
)
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from temporal_agent_harness.harness import agent
from temporal_agent_harness.harness.agent_client import (
    AgentClient,
    AgentTurnError,
    ResumePoint,
)
from temporal_agent_harness.harness.agent_protocol import (
    SEND_SUBAGENT_MESSAGE_UPDATE,
    AgentConfig,
    AgentEvent,
    AgentEventType,
    AgentMessage,
    AgentMessageReply,
    AgentStream,
    MidTurn,
    ReplyDelta,
    SubagentDispatch,
    SubagentMessage,
    TextMessage,
    TextReply,
    ToolApprovalPolicy,
)
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner
from temporal_agent_harness.harness.stream_context import StreamBackend, TurnStreamContext
from temporal_agent_harness.plugin import AgentHarnessPlugin
from tests._streams import STORE, agent_stream

needs_a_stream = pytest.mark.skipif(STORE is None, reason="only a Stream on a Stream Store")


def _has_streams_api() -> bool:
    # A released SDK has no ``temporalio.nexus.system``, so the parent import fails first.
    try:
        return importlib.util.find_spec("temporalio.nexus.system.stream_service") is not None
    except ImportError:
        return False


has_streams_api = _has_streams_api()

# How many times each text went through ``flaky_tool``, so its first attempt can fail.
_TOOL_ATTEMPTS: dict[str, int] = {}
# The tags whose ``fail_task_once`` update already failed a Workflow Task.
_FAILED_TASKS: set[str] = set()


@agent.activity_tool_defn()
async def flaky_tool(text: str) -> str:
    """Fails its first attempt for each text, as a model call that hits a rate limit does."""
    _TOOL_ATTEMPTS[text] = _TOOL_ATTEMPTS.get(text, 0) + 1
    if _TOOL_ATTEMPTS[text] == 1:
        raise RuntimeError("the first attempt fails")
    return f"ok:{text}"


def _runner(config: AgentConfig) -> AgentWorkflowRunner:
    return AgentWorkflowRunner(
        config,
        stream=agent_stream(),
        approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
    )


@agent.defn
class BackendChild:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = _runner(config)
        self._joined = False

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def ask(self, message: TextMessage) -> TextReply:
        """Answer, after a join when asked to wait for one."""
        if message.text == "hang":
            await workflow.wait_condition(lambda: False)
        if message.text == "sleep":
            await workflow.sleep(1)
        if message.text == "wait-for-join":
            await workflow.wait_condition(lambda: self._joined)
        if message.text == "fail":
            raise ValueError("ours blew up")
        return TextReply(text="ours")

    @agent.accepts(mid_turn=MidTurn.ACCEPT)
    async def join(self, message: TextMessage) -> TextReply:
        """Join the open turn."""
        self._joined = True
        if message.text == "fail":
            raise ValueError("theirs blew up")
        return TextReply(text="theirs")


@agent.defn
class BackendParent:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = _runner(config)
        self._child: str | None = None

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def delegate(self, message: TextMessage) -> TextReply:
        """Ask a subagent, started on first use."""
        if self._child is None:
            self._child = await self._runner.start_subagent(
                "child", "BackendChild", workflow.info().task_queue
            )
        try:
            out = await self._runner.run_subagent_turn(
                self._child, "ask", {"text": message.text}
            )
        except ActivityError as e:
            return TextReply(text=f"error:{getattr(e.cause, 'type', None)}:{e.cause}")
        return TextReply(text=out["text"])

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def use_tool(self, message: TextMessage) -> TextReply:
        """Run the flaky tool once."""
        return TextReply(text=await self._runner.run_tool("t1", flaky_tool, message.text))

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def echo(self, message: TextMessage) -> TextReply:
        """Answer at once."""
        return TextReply(text=message.text)

    @workflow.query
    def child(self) -> str | None:
        """The subagent's workflow id, once it runs."""
        status = self._runner._status.to_agent_status()
        return status.subagents[0].workflow_id if status.subagents else None

    @workflow.update
    async def fail_task_once(self, tag: str) -> None:
        """Write a record, then fail the Workflow Task the first time, after the write."""
        sink = self._runner._sink
        before = sink.accepted_cursor()
        self._runner._pub("", 0, ReplyDelta(text=tag))
        await workflow.wait_condition(lambda: sink.accepted_cursor() != before)
        if tag not in _FAILED_TASKS:
            _FAILED_TASKS.add(tag)
            raise RuntimeError("fail this Workflow Task")



@workflow.defn
class ContinuingAgent:
    """Continues as new the first time it is closed, so a test reads both runs."""

    @workflow.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = _runner(config)

    @workflow.run
    async def run(self, config: AgentConfig) -> None:
        await self._runner.run(self)
        if config.stream is None:
            workflow.continue_as_new(self._runner.continue_as_new_config(config))

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def echo(self, message: TextMessage) -> TextReply:
        """Answer at once."""
        return TextReply(text=message.text)


@workflow.defn
class PlainContinuingAgent:
    """Continues as new without ``continue_as_new_config``, so its next run counts from zero."""

    @workflow.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = _runner(config)

    @workflow.run
    async def run(self, config: AgentConfig) -> None:
        await self._runner.run(self)
        if workflow.info().continued_run_id is None:
            workflow.continue_as_new(config)

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def echo(self, message: TextMessage) -> TextReply:
        """Answer at once."""
        return TextReply(text=message.text)


class _UnusedBackend(StreamBackend):
    """A backend that fails if an agent opens it."""

    def open_sink(self) -> Any:
        raise AssertionError("a subagent publishes where its parent does")


@agent.defn
class OtherBackendChild:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=_UnusedBackend(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def ask(self, message: TextMessage) -> TextReply:
        """Answer."""
        return TextReply(text="child:" + message.text)


@agent.defn
class WorkflowStreamsParent:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def delegate(self, message: TextMessage) -> TextReply:
        """Ask a subagent that passes a ``stream=`` of another backend."""
        handle = await self._runner.start_subagent(
            "child", "OtherBackendChild", workflow.info().task_queue
        )
        out = await self._runner.run_subagent_turn(handle, "ask", {"text": message.text})
        return TextReply(text=out["text"])


@workflow.defn
class RefusedWrite:
    """Publishes through a ``ContribSink`` on a Stream that nothing created."""

    @workflow.run
    async def run(self) -> str:
        from temporal_agent_harness.harness import contrib_stream
        from temporal_agent_harness.harness.agent_protocol import AgentStream

        ref = {"id": "missing:run1:agent_events"}
        sink = contrib_stream.open_place(AgentStream(ref=ref, agent_id="a"))
        event = AgentEvent(
            agent_id="a", turn_id="t", turn_number=1, timestamp=0.0, event=ReplyDelta(text="x")
        )
        sink.publish(event)
        # The refusal costs the event, so the sink settles and the run goes on.
        await sink.settled()
        return "done"


@workflow.defn
class BurstWriter:
    """Publishes two bursts through a ``ContribSink``, the second while the first is written."""

    @workflow.run
    async def run(self, ref: dict[str, Any], first: int, second: int) -> str:
        from temporal_agent_harness.harness import contrib_stream
        from temporal_agent_harness.harness.agent_protocol import AgentStream

        sink = contrib_stream.open_place(AgentStream(ref=ref, agent_id="a"))
        for n in range(first):
            sink.publish(_numbered_event(n))
        # The writer takes the first burst, and its append is in flight during the second.
        await asyncio.sleep(0)
        for n in range(first, first + second):
            sink.publish(_numbered_event(n))
        await sink.settled()
        return sink.accepted_cursor()


def _numbered_event(n: int) -> AgentEvent:
    return AgentEvent(
        agent_id="a", turn_id="t", turn_number=1, timestamp=0.0, event=ReplyDelta(text=str(n))
    )


@pytest_asyncio.fixture
async def client_and_queue():
    env = await WorkflowEnvironment.start_time_skipping(data_converter=pydantic_data_converter)
    task_queue = f"stream-backends-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[
            BackendParent,
            BackendChild,
            ContinuingAgent,
            PlainContinuingAgent,
            WorkflowStreamsParent,
            OtherBackendChild,
            RefusedWrite,
            BurstWriter,
        ],
        plugins=[AgentHarnessPlugin(tools=[flaky_tool])],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        try:
            yield env.client, task_queue
        finally:
            await env.shutdown()


async def _start(client: Client, task_queue: str) -> AgentClient:
    handle = await client.start_workflow(
        BackendParent.run,
        AgentConfig(),
        id=f"BackendParent-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    return AgentClient(client, handle.id)


async def _turn(agent_client: AgentClient, handler: str, text: str, **kwargs: Any) -> list[Any]:
    stream = await agent_client.send_message(
        handler, {"text": text}, on_item=lambda item, point: (item, point), **kwargs
    )
    async with asyncio.timeout(30):
        return [item async for item in stream]


async def _running_child(client: Client, parent: AgentClient) -> str:
    """The parent's subagent's workflow id, once the subagent is in a turn."""
    handle = client.get_workflow_handle(parent.workflow_id)
    async with asyncio.timeout(30):
        while (child := await handle.query(BackendParent.child)) is None:
            await asyncio.sleep(0.05)
        while not (await AgentClient(client, child).get_status()).turn_active:
            await asyncio.sleep(0.05)
    return child


def _reply(items: list[Any]) -> str:
    return next(
        item.event.output["text"]
        for item, _ in items
        if isinstance(item, AgentEvent) and item.event.type == AgentEventType.MESSAGE_HANDLER_END
    )


@needs_a_stream
async def test_a_refused_write_costs_the_event_and_not_the_run(client_and_queue):
    client, task_queue = client_and_queue
    async with asyncio.timeout(30):
        result = await client.execute_workflow(
            RefusedWrite.run, id=f"RefusedWrite-{uuid.uuid4()}", task_queue=task_queue
        )
    assert result == "done"


@needs_a_stream
async def test_a_burst_of_workflow_events_keeps_its_order_in_one_append_at_a_time(
    client_and_queue,
):
    client, task_queue = client_and_queue
    stream = await client.create_stream(id=f"burst-{uuid.uuid4()}", body_type=AgentEvent)
    ref = {"id": stream.ref.id, "store": stream.ref.store}
    async with asyncio.timeout(30):
        cursor = await client.execute_workflow(
            BurstWriter.run,
            args=[ref, 25, 15],
            id=f"BurstWriter-{uuid.uuid4()}",
            task_queue=task_queue,
        )

    records = [record async for record in stream.iter_read_items()]
    assert [record.body.event.text for record in records] == [str(n) for n in range(40)]
    # Each append is one Local Activity, which writes as its own Producer. A burst is one append.
    producers = [record.producer.id for record in records if record.producer is not None]
    assert len(producers) == 40 and len(set(producers)) == 2
    assert producers[:25] == [producers[0]] * 25
    assert cursor == records[-1].offset.hex()


async def test_a_repeated_update_id_streams_the_original_turn(client_and_queue):
    client, task_queue = client_and_queue
    agent_client = await _start(client, task_queue)
    first = await _turn(agent_client, "echo", "one", update_id="send-1")
    await _turn(agent_client, "echo", "two", update_id="send-2")

    repeat = await _turn(agent_client, "echo", "one", update_id="send-1")

    assert _reply(repeat) == _reply(first) == "one"
    assert not any(isinstance(item, AgentTurnError) for item, _ in repeat)


async def test_a_subagent_answer_is_its_own_message_s_in_a_shared_turn(client_and_queue):
    client, task_queue = client_and_queue
    parent = await _start(client, task_queue)
    reply = asyncio.create_task(_turn(parent, "delegate", "wait-for-join"))
    child = await _running_child(client, parent)
    await AgentClient(client, child).submit_message("join", {"text": "fail"})

    assert _reply(await reply) == "ours"


async def test_a_subagent_s_own_failure_fails_the_parent_s_call(client_and_queue):
    client, task_queue = client_and_queue
    items = await _turn(await _start(client, task_queue), "delegate", "fail")

    reply = _reply(items)
    assert reply.startswith("error:SubagentTurnError:") and "ours blew up" in reply
    # The child ran the turn and failed it, so the parent still closes the turn's bracket.
    parent_id = items[0][0].agent_id
    assert (parent_id, AgentEventType.SUBAGENT_REPLY_RECEIVED) in [
        (item.agent_id, item.event.type) for item, _ in items if isinstance(item, AgentEvent)
    ]


async def test_a_terminated_subagent_ends_the_parent_s_call(client_and_queue):
    client, task_queue = client_and_queue
    parent = await _start(client, task_queue)
    reply = asyncio.create_task(_turn(parent, "delegate", "hang"))
    child = await _running_child(client, parent)
    await client.get_workflow_handle(child).terminate()

    assert _reply(await reply).startswith("error:SubagentNoReply:")


async def test_a_subagent_closed_mid_turn_still_answers(client_and_queue):
    client, task_queue = client_and_queue
    parent = await _start(client, task_queue)
    reply = asyncio.create_task(_turn(parent, "delegate", "sleep"))
    child = await _running_child(client, parent)
    await client.get_workflow_handle(child).signal("close")

    assert _reply(await reply) == "ours"


async def test_only_the_parent_sends_send_subagent_message(client_and_queue):
    client, task_queue = client_and_queue
    target = await _start(client, task_queue)
    status = await target.get_status()
    forged = SubagentMessage(
        message=AgentMessage(type="echo", payload={"text": "x"}),
        dispatch=SubagentDispatch(
            agent_key="child",
            parent_stream_context=TurnStreamContext(
                turn_id="t", turn_number=1, agent_id=status.agent_id
            ),
        ),
    )
    with pytest.raises(WorkflowUpdateFailedError) as raised:
        await client.get_workflow_handle(target.workflow_id).execute_update(
            SEND_SUBAGENT_MESSAGE_UPDATE, forged, result_type=AgentMessageReply
        )
    assert getattr(raised.value.cause, "type", None) == "NotTheParent"


async def test_a_workflow_streams_parent_keeps_a_subagent_of_another_backend_on_its_stream(
    client_and_queue,
):
    client, task_queue = client_and_queue
    handle = await client.start_workflow(
        WorkflowStreamsParent.run,
        AgentConfig(),
        id=f"WorkflowStreamsParent-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    items = await _turn(AgentClient(client, handle.id), "delegate", "hi")

    assert _reply(items) == "child:hi"
    # The client merges a subagent's events only from the subagent's own Workflow Streams log.
    events = [item for item, _ in items if isinstance(item, AgentEvent)]
    assert any(event.agent_id != events[0].agent_id for event in events)


@needs_a_stream
async def test_a_subagent_turn_comes_between_its_markers_on_the_shared_stream(
    client_and_queue,
):
    client, task_queue = client_and_queue
    items = await _turn(await _start(client, task_queue), "delegate", "hi")

    kinds = [(item.agent_id, item.event.type) for item, _ in items]
    parent_id = kinds[0][0]
    sent = kinds.index((parent_id, AgentEventType.SUBAGENT_MESSAGE_SENT))
    received = kinds.index((parent_id, AgentEventType.SUBAGENT_REPLY_RECEIVED))
    child = [i for i, (agent_id, _) in enumerate(kinds) if agent_id != parent_id]
    assert child and sent < min(child) and max(child) < received
    assert kinds[max(child)][1] == AgentEventType.TURN_END


@needs_a_stream
async def test_a_retried_tool_withdraws_exactly_its_first_attempt(client_and_queue):
    client, task_queue = client_and_queue
    text = f"flaky-{uuid.uuid4()}"
    items = await _turn(await _start(client, task_queue), "use_tool", text)

    first = [
        point.offset
        for item, point in items
        if item.event.type in (AgentEventType.TOOL_START, AgentEventType.TOOL_ERROR)
    ][:2]
    markers = [item for item, _ in items if item.event.type == AgentEventType.ATTEMPT_SUPERSEDED]
    assert len(markers) == 1
    # The marker's ``Superseded`` item shares an offset with the retry's first record, which a
    # client tells apart by its offset, so the marker reports none.
    assert [point.offset for item, point in items if item in markers] == [""]
    assert markers[0].event.withdrawn_offsets == first
    assert markers[0].agent_id == items[0][0].agent_id
    assert _reply(items) == f"ok:{text}"


@needs_a_stream
async def test_resuming_at_a_marker_keeps_the_record_it_shares_an_offset_with(
    client_and_queue,
):
    client, task_queue = client_and_queue
    agent_client = await _start(client, task_queue)
    tag = f"retried-{uuid.uuid4()}"
    await client.get_workflow_handle(agent_client.workflow_id).execute_update(
        BackendParent.fail_task_once, tag
    )

    from temporal_agent_harness.harness.contrib_stream import read_turn_events

    status = await agent_client.get_status()
    assert status.stream is not None

    async def read(after: str, deltas: int) -> list[Any]:
        found = []
        items = read_turn_events(
            client, status.stream, workflow_id=agent_client.workflow_id, after=after
        )
        try:
            async with asyncio.timeout(5):
                async for item in items:
                    found.append(item)
                    kinds = [f.event.event.type for f in found]
                    if kinds.count(AgentEventType.REPLY_DELTA) == deltas:
                        break
        except TimeoutError:
            pass
        finally:
            await items.aclose()
        return found

    everything = await read("", deltas=2)
    marker = next(
        item for item in everything if item.event.event.type == AgentEventType.ATTEMPT_SUPERSEDED
    )
    after_marker = await read(marker.cursor, deltas=1)
    # The rewritten record has the marker's offset, so a resume at the marker still gets it,
    # and not the marker again.
    assert [item.event.event.type for item in after_marker][:1] == [AgentEventType.REPLY_DELTA]
    # A client tells events apart by their offsets, so the marker has none of its own.
    assert marker.offset == "" and after_marker[0].offset


@needs_a_stream
async def test_attach_reads_through_continue_as_new(client_and_queue):
    client, task_queue = client_and_queue
    handle = await client.start_workflow(
        ContinuingAgent.run,
        AgentConfig(),
        id=f"ContinuingAgent-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    agent_client = AgentClient(client, handle.id)
    await _turn(agent_client, "echo", "before")
    await handle.signal("close")
    async with asyncio.timeout(30):
        while (await handle.describe()).run_id == handle.result_run_id:
            await asyncio.sleep(0.05)
    await _turn(agent_client, "echo", "after")

    stream = await agent_client.attach(on_item=lambda item, point: item)
    async with asyncio.timeout(30):
        texts = [
            item.event.output["text"]
            async for item in stream
            if item.event.type == AgentEventType.MESSAGE_HANDLER_END
        ]
    assert texts == ["before", "after"]


async def _continued(client: Client, handle: Any) -> None:
    """Close the first run of a continuing agent, and wait for its next run."""
    await handle.signal("close")
    async with asyncio.timeout(30):
        while (await handle.describe()).run_id == handle.result_run_id:
            await asyncio.sleep(0.05)


@needs_a_stream
async def test_attach_after_a_plain_continue_as_new_streams_the_new_turns(client_and_queue):
    """The next run counts ``seq`` from zero, so a saved point's ``seq`` tells nothing there."""
    client, task_queue = client_and_queue
    handle = await client.start_workflow(
        PlainContinuingAgent.run,
        AgentConfig(),
        id=f"PlainContinuingAgent-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    agent_client = AgentClient(client, handle.id)
    for text in ("one", "two", "three"):
        turn = await _turn(agent_client, "echo", text)
    saved = turn[-1][1].encode()
    await _continued(client, handle)
    await _turn(agent_client, "echo", "after")

    stream = await agent_client.attach(on_item=lambda item, point: item, resume=saved)
    async with asyncio.timeout(30):
        texts = [
            item.event.output["text"]
            async for item in stream
            if item.event.type == AgentEventType.MESSAGE_HANDLER_END
        ]
    assert texts == ["after"]


@needs_a_stream
async def test_the_first_send_after_continue_as_new_starts_past_the_earlier_turns(
    client_and_queue,
):
    client, task_queue = client_and_queue
    handle = await client.start_workflow(
        ContinuingAgent.run,
        AgentConfig(),
        id=f"ContinuingAgent-{uuid.uuid4()}",
        task_queue=task_queue,
    )
    agent_client = AgentClient(client, handle.id)
    for text in ("one", "two"):
        await _turn(agent_client, "echo", text)
    await _continued(client, handle)

    reply = await agent_client.submit_message("echo", {"text": "after"})
    out = await _poll(_nexus_handler(client, task_queue), handle.id, reply.accepted_cursor)
    accepted = [
        event.event.payload["text"]
        for event in (_payload_converter_event(item.data) for item in out.items)
        if event.event.type == AgentEventType.MESSAGE_ACCEPTED
    ]
    assert reply.accepted_cursor and accepted == ["after"]


def _tree_event(agent_id: str) -> AgentEvent:
    return AgentEvent(
        agent_id=agent_id, turn_id="t", turn_number=1, timestamp=0.0, event=ReplyDelta(text="x")
    )


@needs_a_stream
async def test_a_nexus_poll_gives_a_cursor_past_other_agents_records(client_and_queue):
    """A batch with no item of its agent gives a cursor past the records that it read, so a
    later poll doesn't read them again."""
    from temporal_agent_harness.nexus_agent_adapter.generated import PollMessagesInput

    client, task_queue = client_and_queue
    stream = await client.create_stream(id=f"tree-{uuid.uuid4()}", body_type=AgentEvent)
    root = await stream.append(_tree_event("root"))
    head = await stream.append(*[_tree_event("root-child") for _ in range(20)])
    assert root.last_offset and head.last_offset
    place = AgentStream(ref={"id": stream.ref.id, "store": stream.ref.store}, agent_id="root")
    out = await _nexus_handler(client, task_queue)._poll_stream(
        "gone",
        place,
        PollMessagesInput(
            session_id="gone",
            cursor=0,
            stream_cursor=root.last_offset.hex(),
        ),
        timeout=1.0,
    )
    assert out.items == [] and out.next_cursor == head.last_offset.hex()


@needs_a_stream
async def test_a_cursor_that_the_retention_passed_is_a_cursor_error(client_and_queue):
    from temporal_agent_harness.harness import contrib_stream

    from temporalio.client import StreamConfig

    lane, _ = client_and_queue
    # The Retention is a setting of the Store, so this client has one of its own.
    client = Client(
        **{**lane.config(), "stream_config": StreamConfig(stores=[_short_retention_store()])}
    )
    stream = await client.create_stream(id=f"tree-{uuid.uuid4()}", body_type=AgentEvent)
    first = await stream.append(_tree_event("root"))
    await stream.append(_tree_event("root"))
    assert first.last_offset
    # The Retention counts from the close, so the closed Stream expires a second later.
    await stream.close()
    await asyncio.sleep(1.5)
    place = AgentStream(ref={"id": stream.ref.id, "store": stream.ref.store}, agent_id="root")
    with pytest.raises(contrib_stream.TurnCursorError):
        await contrib_stream.check_turn_cursor(client, place, first.last_offset.hex())
    with pytest.raises(contrib_stream.TurnCursorError):
        async for _ in contrib_stream.read_turn_events(
            client, place, workflow_id="gone", after=first.last_offset.hex()
        ):
            pass


@needs_a_stream
async def test_a_cursor_with_an_offset_of_the_wrong_length_is_a_cursor_error(client_and_queue):
    from temporal_agent_harness.harness import contrib_stream

    client, _ = client_and_queue
    stream = await client.create_stream(id=f"tree-{uuid.uuid4()}", body_type=AgentEvent)
    await stream.append(_tree_event("root"))
    place = AgentStream(ref={"id": stream.ref.id, "store": stream.ref.store}, agent_id="root")
    # A Store offset has at most 8 bytes, so a 16 byte one is no position on the Stream.
    cursor = "00" * 16
    with pytest.raises(contrib_stream.TurnCursorError):
        await contrib_stream.check_turn_cursor(client, place, cursor)
    with pytest.raises(contrib_stream.TurnCursorError):
        async for _ in contrib_stream.read_turn_events(
            client, place, workflow_id="gone", after=cursor
        ):
            pass


@needs_a_stream
async def test_a_read_waits_for_a_create_in_flight_while_the_agent_runs(client_and_queue):
    """A status names a root's Stream before its create returns, so a read can come first."""
    from temporal_agent_harness.harness import contrib_stream

    client, task_queue = client_and_queue
    running = await _start(client, task_queue)
    stream_id = f"tree-{uuid.uuid4()}"
    place = AgentStream(ref={"id": stream_id, "store": None}, agent_id="root")
    items = contrib_stream.read_turn_events(client, place, workflow_id=running.workflow_id)
    first = asyncio.ensure_future(anext(items))
    await asyncio.sleep(0.3)
    assert not first.done()

    stream = await client.create_stream(id=stream_id, body_type=AgentEvent)
    await stream.append(_tree_event("root"))
    async with asyncio.timeout(10):
        item = await first
    await items.aclose()
    assert item.event.agent_id == "root"

    # A Stream that never comes is one whose agent ended before its create.
    gone = AgentStream(ref={"id": f"tree-{uuid.uuid4()}", "store": None}, agent_id="root")
    async with asyncio.timeout(10):
        ended = contrib_stream.read_turn_events(client, gone, workflow_id="gone")
        assert [item async for item in ended] == []


def _short_retention_store() -> Any:
    """A Store of the lane's kind whose Streams expire one second after their close."""
    retention = timedelta(seconds=1)
    if STORE == "memory":
        from temporalio.testing import MemoryStreamStore

        return MemoryStreamStore(retention=retention)
    from temporalio.contrib.redis.streams import RedisStreamStore

    from temporal_agent_harness.harness.contrib_stream import (
        DEFAULT_REDIS_URL,
        KEY_PREFIX_ENV,
        REDIS_URL_ENV,
    )

    return RedisStreamStore(
        os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL),
        key_prefix=os.environ.get(KEY_PREFIX_ENV) or None,
        retention=retention,
    )


async def test_attach_to_an_agent_terminated_mid_turn_ends(client_and_queue):
    """A terminated run closes nothing on Redis or memory, so the read has to find out."""
    client, task_queue = client_and_queue
    parent = await _start(client, task_queue)
    reply = asyncio.create_task(_turn(parent, "delegate", "hang"))
    await _running_child(client, parent)
    await client.get_workflow_handle(parent.workflow_id).terminate()
    reply.cancel()

    stream = await parent.attach(on_item=lambda item, point: item)
    async with asyncio.timeout(30):
        replayed = [item async for item in stream]
    assert AgentEventType.TURN_END not in [item.event.type for item in replayed]


async def test_attach_at_the_head_of_an_idle_agent_gives_nothing(client_and_queue):
    """A point with no seq, such as a send's ``resume`` or a ``from_offset``, is caught up when
    nothing was written past it, and the attach must not wait for the next turn."""
    client, task_queue = client_and_queue
    agent_client = await _start(client, task_queue)
    turn = await _turn(agent_client, "echo", "hi")

    stream = await agent_client.attach(
        on_item=lambda item, point: item, resume=ResumePoint(cursor=turn[-1][1].cursor).encode()
    )
    async with asyncio.timeout(10):
        assert [item async for item in stream] == []


def _nexus_handler(client: Client, task_queue: str) -> Any:
    # The Bazel build leaves the Nexus adapter out.
    handler = pytest.importorskip(
        "temporal_agent_harness.nexus_agent_adapter.handler", exc_type=ModuleNotFoundError
    )
    return handler.AgentServiceHandler(
        client,
        handler.Config(
            agent_task_queue=task_queue, workflow_name="BackendParent", workflow_id_prefix=""
        ),
    )


async def _poll(handler: Any, workflow_id: str, cursor: str | None) -> Any:
    from temporal_agent_harness.nexus_agent_adapter.generated import PollMessagesInput

    place = (await AgentClient(handler._client, workflow_id).get_status()).stream
    return await handler._poll_stream(
        workflow_id,
        place,
        PollMessagesInput(session_id=workflow_id, cursor=0, stream_cursor=cursor),
        timeout=2.0,
    )


@needs_a_stream
async def test_a_nexus_poll_gives_the_last_batch_before_it_says_closed(client_and_queue):
    client, task_queue = client_and_queue
    agent_client = await _start(client, task_queue)
    await _turn(agent_client, "echo", "bye")
    handle = client.get_workflow_handle(agent_client.workflow_id)
    await handle.signal("close")
    await handle.result()
    handler = _nexus_handler(client, task_queue)

    first = await _poll(handler, agent_client.workflow_id, None)
    then = await _poll(handler, agent_client.workflow_id, first.next_cursor)

    assert first.items and not first.closed
    assert not then.items and then.closed


@needs_a_stream
async def test_a_later_nexus_poll_withdraws_what_an_earlier_one_gave(client_and_queue):
    from temporal_agent_harness.harness.contrib_stream import read_turn_events
    from temporal_agent_harness.nexus_agent_adapter.generated import (
        SendAgentMessageInput,
        SendAgentMessageInputPayload,
    )

    client, task_queue = client_and_queue
    handler = _nexus_handler(client, task_queue)
    session = f"BackendParent-{uuid.uuid4()}"
    sent = await handler.send_agent_message(
        SimpleNamespace(request_id=str(uuid.uuid4())),
        SendAgentMessageInput(
            session_id=session,
            msg_type="use_tool",
            payload=SendAgentMessageInputPayload(
                additional_properties={"text": f"flaky-{uuid.uuid4()}"}
            ),
        ),
    )
    # The retry waits out the activity's backoff, so the poll that gives attempt 1 ends first.
    polls: list[list[AgentEventType]] = []
    markers: list[AgentEvent] = []
    cursor = sent.stream_cursor
    async with asyncio.timeout(30):
        while AgentEventType.TURN_END not in sum(polls, []):
            out = await _poll(handler, session, cursor)
            events = [_payload_converter_event(item.data) for item in out.items]
            polls.append([e.event.type for e in events])
            markers += [e for e in events if e.event.type == AgentEventType.ATTEMPT_SUPERSEDED]
            cursor = out.next_cursor

    status = await AgentClient(client, session).get_status()
    first: list[str] = []
    items = read_turn_events(client, status.stream, workflow_id=session)
    async with asyncio.timeout(30):
        async for item in items:
            if item.event.event.type in (AgentEventType.TOOL_START, AgentEventType.TOOL_ERROR):
                first.append(item.offset)
            if item.event.event.type == AgentEventType.TURN_END:
                break
    await items.aclose()

    shown = next(i for i, kinds in enumerate(polls) if AgentEventType.TOOL_ERROR in kinds)
    marked = next(i for i, kinds in enumerate(polls) if AgentEventType.ATTEMPT_SUPERSEDED in kinds)
    assert shown < marked
    assert markers[0].event.withdrawn_offsets == first[:2]


@needs_a_stream
async def test_the_nexus_poll_after_a_marker_does_not_give_it_again(client_and_queue, monkeypatch):
    from temporal_agent_harness.nexus_agent_adapter import handler as nexus_handler

    client, task_queue = client_and_queue
    agent_client = await _start(client, task_queue)
    await client.get_workflow_handle(agent_client.workflow_id).execute_update(
        BackendParent.fail_task_once, f"retried-{uuid.uuid4()}"
    )
    handler = _nexus_handler(client, task_queue)

    def kinds(out: Any) -> list[AgentEventType]:
        return [_payload_converter_event(item.data).event.type for item in out.items]

    everything = kinds(await _poll(handler, agent_client.workflow_id, None))
    # End the first batch at the marker.
    marker = everything.index(AgentEventType.ATTEMPT_SUPERSEDED)
    monkeypatch.setattr(nexus_handler, "_STREAM_POLL_BATCH_LIMIT", marker + 1)
    first = await _poll(handler, agent_client.workflow_id, None)
    then = await _poll(handler, agent_client.workflow_id, first.next_cursor)

    assert kinds(first)[-1] == AgentEventType.ATTEMPT_SUPERSEDED
    assert AgentEventType.ATTEMPT_SUPERSEDED not in kinds(then)


@needs_a_stream
async def test_a_nexus_send_gives_the_cursor_where_its_turn_starts(client_and_queue):
    from temporal_agent_harness.nexus_agent_adapter.generated import (
        SendAgentMessageInput,
        SendAgentMessageInputPayload,
    )

    client, task_queue = client_and_queue
    handler = _nexus_handler(client, task_queue)
    session = f"BackendParent-{uuid.uuid4()}"

    async def send(text: str) -> Any:
        payload = SendAgentMessageInputPayload(additional_properties={"text": text})
        return await handler.send_agent_message(
            SimpleNamespace(request_id=str(uuid.uuid4())),
            SendAgentMessageInput(session_id=session, msg_type="echo", payload=payload),
        )

    await send("one")
    await _turn(AgentClient(client, session), "echo", "two")
    sent = await send("three")
    out = await _poll(handler, session, sent.stream_cursor)

    events = [_payload_converter_event(item.data) for item in out.items]
    assert sent.stream_cursor
    assert {e.turn_number for e in events} == {sent.turn_number}


def _payload_converter_event(data: str) -> AgentEvent:
    import base64

    from temporalio.api.common.v1 import Payload

    payload = Payload()
    payload.ParseFromString(base64.b64decode(data))
    [event] = pydantic_data_converter.payload_converter.from_payloads([payload], [AgentEvent])
    return event


# The agents of histories/workflow_streams_subagent.json, a Workflow Streams session that a harness
# without ``AgentEvent.seq`` recorded. A change to them breaks the replay.
@agent.defn
class ReplayChild:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def ask(self, message: TextMessage) -> TextReply:
        """Answer."""
        return TextReply(text="child:" + message.text)


@agent.defn
class ReplayParent:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def delegate(self, message: TextMessage) -> TextReply:
        """Ask a subagent twice."""
        handle = await self._runner.start_subagent(
            "child", "ReplayChild", workflow.info().task_queue
        )
        out = await self._runner.run_subagent_turn(handle, "ask", {"text": message.text})
        out = await self._runner.run_subagent_turn(handle, "ask", {"text": out["text"]})
        await self._runner.stop_subagent(handle)
        return TextReply(text=out["text"])


async def test_a_recorded_workflow_streams_session_replays():
    """A Workflow Streams session that a harness without ``AgentEvent.seq`` recorded replays on
    this code, so an open session keeps running across the deploy."""
    recorded = json.loads(
        (Path(__file__).parent / "histories" / "workflow_streams_subagent.json").read_text()
    )
    replayer = Replayer(
        workflows=[ReplayParent, ReplayChild],
        data_converter=pydantic_data_converter,
        workflow_runner=UnsandboxedWorkflowRunner(),
    )
    for name, history in recorded.items():
        await replayer.replay_workflow(WorkflowHistory.from_json(f"wf-{name}", history))


def _contrib_stream_choice() -> Any:
    from temporal_agent_harness.harness.contrib_stream import ContribStream

    return ContribStream()


# The agents of histories/contrib_stream_subagent.json, a session on a memory Stream. The SDK ran
# each create and append in a Local Activity there, so a change to what they publish breaks the
# replay.
@agent.defn
class ContribReplayChild:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=_contrib_stream_choice(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def ask(self, message: TextMessage) -> TextReply:
        """Answer."""
        return TextReply(text="child:" + message.text)


@agent.defn
class ContribReplayParent:
    @agent.init
    def __init__(self, config: AgentConfig) -> None:
        self._runner = AgentWorkflowRunner(
            config,
            stream=_contrib_stream_choice(),
            approval_policy_default=ToolApprovalPolicy.dangerously_skip_all(),
        )

    @agent.accepts(mid_turn=MidTurn.ENQUEUE)
    async def delegate(self, message: TextMessage) -> TextReply:
        """Ask a subagent twice."""
        handle = await self._runner.start_subagent(
            "child", "ContribReplayChild", workflow.info().task_queue
        )
        out = await self._runner.run_subagent_turn(handle, "ask", {"text": message.text})
        out = await self._runner.run_subagent_turn(handle, "ask", {"text": out["text"]})
        await self._runner.stop_subagent(handle)
        return TextReply(text=out["text"])


@needs_a_stream
async def test_a_recorded_contrib_stream_session_replays():
    """A parent and subagent session on a memory Stream replays on this code. The replay takes
    each Stream call's result from its Local Activity marker. It needs a Stream config, since a
    Stream call with none fails the Workflow Task, but it calls no method of the Store."""
    from temporalio.client import StreamConfig
    from temporalio.testing import MemoryStreamStore

    recorded = json.loads(
        (Path(__file__).parent / "histories" / "contrib_stream_subagent.json").read_text()
    )
    histories = {
        name: WorkflowHistory.from_json(f"wf-{name}", history)
        for name, history in recorded.items()
    }
    # The session wrote through the Local Activity that the SDK registers for the memory Store.
    assert {
        activity_type
        for history in histories.values()
        for activity_type in _local_activity_types(history)
    } == {"__temporal_stream_store_call:memory"}
    replayer = Replayer(
        workflows=[ContribReplayParent, ContribReplayChild],
        data_converter=pydantic_data_converter,
        workflow_runner=UnsandboxedWorkflowRunner(),
        stream_config=StreamConfig(stores=[MemoryStreamStore()]),
    )
    for history in histories.values():
        await replayer.replay_workflow(history)


def _local_activity_types(history: WorkflowHistory) -> list[str]:
    """The Activity type of each Local Activity marker in ``history``."""
    types = []
    for event in history.events:
        marker = event.marker_recorded_event_attributes
        if marker.marker_name == "core_local_activity":
            [data] = marker.details["data"].payloads
            types.append(json.loads(data.data)["activity_type"])
    return types


@needs_a_stream
async def test_a_root_on_a_worker_with_no_store_fails_at_once_and_says_why(client_and_queue):
    """The create fails the run as non-retryable, with the SDK's text of what the Worker's
    client lacks."""
    client, _ = client_and_queue
    bare = Client(**{**client.config(), "stream_config": None})
    task_queue = f"no-store-{uuid.uuid4()}"
    async with Worker(
        bare,
        task_queue=task_queue,
        workflows=[BackendParent],
        plugins=[AgentHarnessPlugin()],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        handle = await bare.start_workflow(
            BackendParent.run,
            AgentConfig(),
            id=f"BackendParent-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        with pytest.raises(WorkflowFailureError) as raised:
            async with asyncio.timeout(30):
                await handle.result()

    causes = []
    failure: BaseException | None = raised.value.cause
    while failure is not None:
        causes.append(failure)
        failure = failure.__cause__ or getattr(failure, "cause", None)
    # The first is the harness's non-retryable error. Its cause is the SDK's own error.
    missing = next(
        cause
        for cause in causes
        if isinstance(cause, ApplicationError) and cause.type == "StreamConfigError"
    )
    assert missing.non_retryable
    assert missing.message == (
        "The client of this Worker has no Stream Store. "
        "Give one in stream_config=StreamConfig(stores=[...])."
    )


class _FailingDescribe:
    """A client whose ``describe`` fails with ``status``."""

    def __init__(self, status: RPCStatusCode) -> None:
        self._status = status

    def get_workflow_handle(self, workflow_id: str) -> Any:
        return self

    async def describe(self) -> Any:
        raise RPCError("describe failed", self._status, b"")


class _UnansweredQuery:
    """A client whose status query fails, of a workflow that has ``status``."""

    def __init__(self, status: WorkflowExecutionStatus) -> None:
        self._status = status

    def get_workflow_handle(self, workflow_id: str, **options: Any) -> Any:
        return self

    def config(self, **options: Any) -> Any:
        # The place found without a query names the client's Store and makes no call.
        from temporalio.client import StreamConfig
        from temporalio.testing import MemoryStreamStore

        return {"stream_config": StreamConfig(stores=[MemoryStreamStore()])}

    async def query(self, *args: Any, **kwargs: Any) -> Any:
        raise RPCError("no worker answered", RPCStatusCode.DEADLINE_EXCEEDED, b"")

    async def describe(self) -> Any:
        info = SimpleNamespace(first_run_id="run1")
        return SimpleNamespace(
            status=self._status, raw_description=SimpleNamespace(workflow_execution_info=info)
        )


async def test_a_nexus_poll_of_a_closed_agent_that_answers_no_query_keeps_its_backend():
    closed = _nexus_handler(_UnansweredQuery(WorkflowExecutionStatus.COMPLETED), "q")
    running = _nexus_handler(_UnansweredQuery(WorkflowExecutionStatus.RUNNING), "q")
    from temporal_agent_harness.nexus_agent_adapter.generated import PollMessagesInput

    def poll(stream_cursor: str | None) -> Any:
        return PollMessagesInput(session_id="wf", cursor=0, stream_cursor=stream_cursor)

    assert await closed._stream_place("wf", poll(None)) is None
    with pytest.raises(RPCError):
        await running._stream_place("wf", poll(None))
    if not has_streams_api:
        pytest.skip("needs the SDK's Streams API")

    place = await closed._stream_place("wf", poll("5"))
    assert place is not None and place.owner() == ("wf", "run1")


@needs_a_stream
async def test_the_place_found_without_a_query_gives_the_agent_s_own_events(client_and_queue):
    from temporal_agent_harness.harness.contrib_stream import read_turn_events, root_place

    client, task_queue = client_and_queue
    agent_client = await _start(client, task_queue)
    await _turn(agent_client, "delegate", "hi")
    status = await agent_client.get_status()
    assert status.stream is not None
    description = await client.get_workflow_handle(agent_client.workflow_id).describe()

    first_run_id = description.raw_description.workflow_execution_info.first_run_id
    place = root_place(client, agent_client.workflow_id, first_run_id)

    async def own(place: Any) -> list[tuple[str, str]]:
        found = []
        items = read_turn_events(client, place, workflow_id=agent_client.workflow_id)
        try:
            async with asyncio.timeout(30):
                async for item in items:
                    found.append((item.event.agent_id, item.offset))
                    if item.event.event.type == AgentEventType.TURN_END:
                        break
        finally:
            await items.aclose()
        return found

    assert place.ref == status.stream.ref
    # The subagent writes the same Stream, and the read leaves its events out.
    reported = await own(status.stream)
    assert reported == await own(place)
    assert {agent_id for agent_id, _ in reported} == {status.agent_id}


async def test_only_a_workflow_that_is_gone_ends_a_read():
    if not has_streams_api:
        pytest.skip("needs the SDK's Streams API")
    from temporal_agent_harness.harness.contrib_stream import _ended

    flaky = _FailingDescribe(RPCStatusCode.UNAVAILABLE)
    gone = _FailingDescribe(RPCStatusCode.NOT_FOUND)
    denied = _FailingDescribe(RPCStatusCode.PERMISSION_DENIED)

    # A transient error doesn't end the read, so it asks again later.
    assert not await _ended(flaky, "wf", (None, None))  # type: ignore[arg-type]
    with pytest.raises(RPCError):
        await _ended(denied, "wf", (None, None))  # type: ignore[arg-type]
    with pytest.raises(RPCError):
        await _nexus_handler(flaky, "q")._running("wf")
    assert await _ended(gone, "wf", (None, None))  # type: ignore[arg-type]
    assert not await _nexus_handler(gone, "q")._running("wf")
