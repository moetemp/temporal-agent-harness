# ABOUTME: Regression tests for a participant coroutine that is finalized from outside its own
# asyncio task. A worker shuts down without evicting an idle cached run, so a participant parked in
# a tool call is closed by the garbage collector later, in whatever context is current at that
# moment, often another workflow's activation on the same worker thread. The teardown must stay a
# teardown: run_tool's ambient resets must not raise, and the participant must publish nothing,
# because the ambient stream writer at that moment belongs to the other workflow.
#
# Run with: uv run pytest tests/harness/test_participant_teardown.py -v

from __future__ import annotations

import contextvars
from types import SimpleNamespace
from typing import Any

import pytest

from temporal_agent_harness.harness import agent_workflow
from temporal_agent_harness.harness.agent_protocol.events import (
    MessageHandlerError,
    MessageHandlerStart,
)


class _Never:
    """An awaitable that parks its awaiter until the coroutine is closed."""

    def __await__(self) -> Any:
        while True:
            yield


async def _parked_tool() -> None:
    await _Never()


def _drive_to_park(coro: Any) -> None:
    # The first step runs in a context of its own, the way a task step does, and sets the
    # ambient tool state there.
    contextvars.copy_context().run(coro.send, None)


def _finalize_elsewhere(coro: Any) -> None:
    # The garbage collector closes the coroutine in whatever context is current, never the one
    # that set the ambient state.
    contextvars.copy_context().run(coro.close)


def test_run_tool_finalized_from_another_context_keeps_the_teardown_clean() -> None:
    coro = agent_workflow.AgentWorkflowRunner.run_tool(SimpleNamespace(), "r1", _parked_tool)
    _drive_to_park(coro)

    # The ambient state was never set in the finalizing context, so there is nothing to
    # restore there and the close has to return without raising.
    _finalize_elsewhere(coro)

    assert agent_workflow._CURRENT_TOOL_ID.get() is None
    assert agent_workflow._CURRENT_RUNNER.get() is None
    assert agent_workflow._CURRENT_TOOL_INJECTIONS.get() is None


def _participant(*, in_own_run: bool, dispatch: Any) -> tuple[Any, list[Any], list[bool]]:
    published: list[Any] = []
    left: list[bool] = []

    def leave_turn() -> bool:
        left.append(True)
        return True

    runner = SimpleNamespace(
        _pub=lambda _turn_id, _turn_number, event, **_kwargs: published.append(event),
        _status=SimpleNamespace(leave_turn=leave_turn),
        _in_own_run=lambda: in_own_run,
    )
    runner._dispatch_turn = lambda agent, admitted, *, joined: dispatch(runner)
    admitted = SimpleNamespace(turn_id="turn-1", turn_number=1, message_id="msg-1")
    coro = agent_workflow.AgentWorkflowRunner._run_participant(
        runner, object(), admitted, joined=False
    )
    return coro, published, left


def test_participant_parked_in_a_tool_call_publishes_nothing_when_finalized() -> None:
    async def dispatch(runner: Any) -> Any:
        return await agent_workflow.AgentWorkflowRunner.run_tool(runner, "r1", _parked_tool)

    coro, published, left = _participant(in_own_run=True, dispatch=dispatch)
    _drive_to_park(coro)
    _finalize_elsewhere(coro)

    assert [type(event) for event in published] == [MessageHandlerStart]
    assert left == [True]


def test_participant_failing_inside_another_run_publishes_nothing() -> None:
    async def dispatch(_runner: Any) -> Any:
        raise RuntimeError("raised while another run's activation is current")

    coro, published, left = _participant(in_own_run=False, dispatch=dispatch)
    with pytest.raises(RuntimeError):
        _drive_to_park(coro)

    assert [type(event) for event in published] == [MessageHandlerStart]
    assert left == [True]


def test_participant_failing_in_its_own_run_reports_the_error() -> None:
    async def dispatch(_runner: Any) -> Any:
        raise RuntimeError("the handler itself failed")

    coro, published, left = _participant(in_own_run=True, dispatch=dispatch)
    with pytest.raises(StopIteration):
        _drive_to_park(coro)

    assert [type(event) for event in published[:2]] == [MessageHandlerStart, MessageHandlerError]
    assert published[1].message == "the handler itself failed"
    assert left == [True]
