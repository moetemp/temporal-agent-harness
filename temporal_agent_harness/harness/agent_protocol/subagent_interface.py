# ABOUTME: The sandbox-safe contract for the subagent-turn activity — the activity NAME, its
# input/output models, and the recommended timeouts. Part of the ``agent_protocol`` package
# (stdlib + pydantic only) so it imports cleanly inside the Temporal workflow sandbox, where
# the parent agent's runner lives and calls ``workflow.execute_activity``. The activity
# IMPLEMENTATION (which needs a Temporal ``Client`` + stream client) lives in
# ``harness/subagent_activities.py`` and is NOT sandbox-safe — the same protocol-vs-client
# split as ``agent_interface`` here vs ``agent_client``.

from __future__ import annotations

from datetime import timedelta
from typing import Any

from pydantic import BaseModel, Field

from temporal_agent_harness.harness.agent_protocol.agent_interface import AgentMessage
from temporal_agent_harness.harness.stream_context import TurnStreamContext

# The registered name of the subagent-turn activity. Used by the activity's ``@activity.defn``
# (in ``subagent_activities.py``) and by the in-workflow ``send_<function>`` subagent tool's
# ``execute_activity`` call (in ``agent_workflow.py``).
RUN_SUBAGENT_TURN_ACTIVITY = "run_subagent_turn"

# The update a parent's subagent-turn activity sends a message with. It is the
# ``send_agent_message`` front door plus the dispatch the child announces on its parent's Topic.
# Not in ``agent_interface``: only the harness sends it.
SEND_SUBAGENT_MESSAGE_UPDATE = "send_subagent_message"

# Steady heartbeat cadence for the activity. The activity heartbeats every interval; the
# wrapper sets ``heartbeat_timeout`` to twice it, leaving one full interval of grace before
# Temporal would consider a silent worker dead. The activity self-derives the interval as
# ``heartbeat_timeout / 2`` (so the two stay consistent however the caller configures it).
DEFAULT_SUBAGENT_HEARTBEAT_INTERVAL = timedelta(seconds=10)
DEFAULT_SUBAGENT_HEARTBEAT_TIMEOUT = DEFAULT_SUBAGENT_HEARTBEAT_INTERVAL * 2

# Generous upper bound on a single subagent turn. Temporal REQUIRES a start_to_close (or
# schedule_to_close) timeout — ``heartbeat_timeout`` alone is rejected — so this is the ceiling
# while ``heartbeat_timeout`` does the real liveness detection. It is intentionally large (a
# subagent turn can run long); a dev wiring the subagent toolset can override it.
DEFAULT_SUBAGENT_START_TO_CLOSE_TIMEOUT = timedelta(hours=1)


class RunSubagentTurnInput(BaseModel):
    """Arguments for one subagent turn — sent to the activity by the parent's in-workflow
    ``send_<function>`` tool. There is deliberately NO turn-timeout field: the turn runs as long
    as the activity is allowed to (bounded only by the caller's ``start_to_close_timeout`` /
    liveness via ``heartbeat_timeout``)."""

    child_workflow_id: str = Field(
        description="The child agent workflow this turn targets — the subagent the parent "
        "started via its start_<key> tool and is now addressing."
    )
    type: str = Field(
        description="The send_agent_message envelope 'type': the name of the target "
        "@agent.accepts handler on the child to route this turn to."
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="The send_agent_message envelope 'payload': the JSON of the target "
        "handler's input model.",
    )
    after_cursor: str = Field(
        default="",
        description="Where to resume reading the child's Topic: the cursor just past the last "
        "record the caller consumed (the previous turn's returned consumed_cursor), or empty "
        "for the beginning. A PERFORMANCE HINT ONLY, never correctness-critical: the streamer "
        "filters to this turn's turn_id, so a stale cursor merely replays a few already-seen "
        "events, and it can never be too far ahead (the next turn's events always follow the "
        "prior turn's turn_end, which is where the prior consumed_cursor points).",
    )
    # The following identify the dispatch for the SubagentMessageSent event the child publishes
    # onto the PARENT's Topic when it admits the message (see SubagentDispatch).
    handle: str = Field(
        description="The parent's short handle for this subagent — for the SubagentMessageSent "
        "event."
    )
    agent_key: str = Field(
        description="The wired agent key of this subagent — for the SubagentMessageSent event."
    )
    parent_stream_context: TurnStreamContext = Field(
        description="The PARENT turn (turn_id + turn_number) the SubagentMessageSent event "
        "belongs to, and the Stream and Topic the parent publishes on."
    )


class SubagentTurnResult(BaseModel):
    """The outcome of one subagent turn — returned by the activity to the parent's
    ``send_<function>`` tool."""

    output: dict[str, Any] = Field(
        default_factory=dict,
        description="The child handler's reply, as the raw JSON dict carried on the child's "
        "``message_handler_end`` for THIS message (selected by ``message_id``, not by turn — a "
        "child turn can carry several participants). The calling send_<function> tool "
        "re-validates it against the handler's statically known output model (boundary "
        "validation).",
    )
    turn_id: str = Field(description="The id of the turn the child actually ran.")
    turn_number: int = Field(
        description="The number of the turn the child actually ran."
    )
    consumed_cursor: str = Field(
        description="The cursor of this turn's turn_end on the child's stream. The caller "
        "stores it and threads it back as the next turn's after_cursor, so each turn streams "
        "from where the last one ended (cheap resume, no full-history replay)."
    )


class SubagentDispatch(BaseModel):
    """What a child needs to announce a parent's message on the parent's Topic.

    The child publishes the parent's ``subagent_message_sent`` in the same Workflow Task that
    admits the message, before its own ``message_accepted``. Both agents write one Stream, so the
    marker comes before every event of the child's turn for every reader.
    """

    agent_key: str = Field(description="The wired agent key of the child.")
    after_cursor: str = Field(
        default="", description="The child Topic cursor the parent's activity reads after."
    )
    parent_stream_context: TurnStreamContext = Field(
        description="The parent turn the marker belongs to, and the parent's Topic."
    )


class SubagentMessage(BaseModel):
    """The payload of the ``send_subagent_message`` update: a message and its dispatch."""

    message: AgentMessage
    dispatch: SubagentDispatch
