# ABOUTME: Internal harness models for activity-side stream publishing. These are NOT protocol
# wire types (not events, not signal/update/query payloads) — they're plumbing the harness uses
# to let an activity publish stream events back to its workflow's turn. Kept in their own leaf
# module (stdlib, pydantic and the SDK's Stream handle only, importing nothing else from
# ``harness``) so BOTH the workflow-side runner (``harness/agent_workflow.py``) AND the
# sandbox-safe activity contracts in ``harness/agent_protocol`` (e.g. ``RunSubagentTurnInput``)
# can embed them without a circular import — and without leaking internal machinery into the
# public ``agent_protocol`` surface.

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

# An SDK release has no ``temporalio.contrib.streams``, and Workflow Streams needs none of it.
# A real handle type is what makes the Stream plugin withdraw an earlier attempt's records when
# a retry starts, so the field takes it when the SDK has it.
if TYPE_CHECKING:
    from temporalio.contrib.streams import ActivityStream

    _ActivityHandle = ActivityStream[Any]
else:
    try:
        from temporalio.contrib.streams import ActivityStream

        _ActivityHandle = ActivityStream[Any]
    except ImportError:
        _ActivityHandle = Any


class StreamBackend:
    """Base of a ``stream=`` choice for ``AgentWorkflowRunner`` other than a ``WorkflowStream``.

    The runner calls ``open_sink`` in ``@agent.init`` for what it publishes through. It tests for
    the method and not the class, since the workflow sandbox can load a second copy of a module.
    """

    def open_sink(self) -> Any:
        raise NotImplementedError


class TurnStreamContext(BaseModel):
    """The bits an activity needs to publish stream events back to a specific turn.

    Read on the workflow side from ``AgentWorkflowRunner.current_stream_context``, serialized
    across the activity boundary in the activity's request payload, and unpacked on the activity
    side by ``AgentWorkflowRunner.publisher_from_activity`` (which targets the activity's own
    parent workflow, or the Stream that ``stream`` names). Callers thread it as one opaque value
    rather than the individual fields.
    """

    turn_id: str = Field(description="The id of the turn to publish against.")
    turn_number: int = Field(description="The monotonic number of that turn.")
    message_id: str | None = Field(
        default=None,
        description="The id of the inbound message whose dispatch this activity is part of — "
        "stamped onto every event the activity publishes (``AgentEvent.message_id``) so a "
        "streamed delta or tool call is attributable to the message that caused it, not merely "
        "to the turn. Resolved workflow-side from the ambient per-participant context when the "
        "carrier is built (``AgentWorkflowRunner.current_stream_context``), so an activity "
        "threads it without knowing it exists. ``None`` when the work belongs to no single "
        "message.",
    )
    agent_id: str = Field(
        description="The short id of the agent that owns this turn — stamped onto every event the "
        "activity publishes (``AgentEvent.agent_id``). Threaded here because an activity can't "
        "derive it from ``activity.info()`` (which only knows the workflow_id), and the agent's id "
        "is no longer its workflow_id but a short, parent-assignable handle."
    )
    stream: _ActivityHandle | None = Field(
        default=None,
        description="The ``temporalio.contrib.streams`` Stream the agent publishes on, or "
        "``None`` on Workflow Streams. An activity gets a Stream only in its input.",
    )
    topic: str = Field(
        default="turn_events",
        description="The Topic the agent publishes on, when ``stream`` is set.",
    )
