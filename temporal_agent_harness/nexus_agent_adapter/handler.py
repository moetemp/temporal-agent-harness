# ABOUTME: Python implementation of the AgentService Nexus handler.
#
# All operations except pollMessages delegate to AgentClient (see _agent_client). pollMessages
# reads the agent's turn events through the stream interface, so the store behind the agent is
# the worker's choice and this handler does not know which one it is.

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass

from nexusrpc import HandlerError, HandlerErrorType
from nexusrpc.handler import StartOperationContext, service_handler, sync_operation
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError
from temporalio.streams import StreamCursorError

from temporal_agent_harness.harness.agent_client import (
    AgentClient,
    CallbackResultError,
    ToolApprovalError,
)
from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS_TOPIC,
    AgentConfig,
    MessageDisposition,
    PendingCallback,
    PendingTurn,
    SubagentInfo,
    ToolApprovalPolicy,
)
from temporal_agent_harness.harness.stream_transport import follow_turn_events

# Aliased with a Nexus* prefix where the name collides with an agent_protocol type of the
# same name but a different (reshaped, wire-friendly) shape.
from .generated import (
    AcceptedFunction as NexusAcceptedFunction,
    AgentInterfaceOutput,
    ApprovalPolicy as NexusApprovalPolicy,
    AgentStatusOutput,
    ApproveToolCallInput,
    ApproveToolCallOutput,
    ExecuteOperatorCommandInput,
    ExecuteOperatorCommandOutput,
    OperatorCommand as NexusOperatorCommand,
    OperatorCommandArgument as NexusOperatorCommandArgument,
    PendingApproval as NexusPendingApproval,
    PendingCallback as NexusPendingCallback,
    PendingTurn as NexusPendingTurn,
    PollMessagesInput,
    PollMessagesOutput,
    ProvideCallbackResultInput,
    ProvideCallbackResultOutput,
    QueryOperatorInterfaceOutput,
    QuerySessionInput,
    SendAgentMessageInput,
    SendMessageOutput,
    StreamItem,
    SubagentInfo as NexusSubagentInfo,
)
from .generated import AgentService as AgentServiceDefinition

DEFAULT_POLL_TIMEOUT_SECONDS = 30.0
# pollMessages is a synchronous Nexus operation, so one call has to answer well inside the
# operation's own deadline; a caller wanting to wait longer polls again.
MAX_POLL_SECONDS = 8.0
POLL_BATCH_LIMIT = 100
# Once one event has arrived, gather what follows it within this window and answer, rather than
# holding the batch open for the whole timeout.
POLL_BATCH_GRACE_SECONDS = 0.05


def _is_workflow_already_completed(exc: Exception) -> bool:
    """True when the target agent workflow has already finished (see poll_messages)."""
    return "already completed" in str(exc).lower()


def _nexus_pending_callback(pc: PendingCallback) -> NexusPendingCallback:
    return NexusPendingCallback(
        tool_id=pc.tool_id,
        tool_name=pc.tool_name,
        tool_input=json.dumps(pc.tool_input),
        output_schema=json.dumps(pc.output_schema),
        turn_number=pc.turn_number,
    )


def _nexus_pending_turn(pt: PendingTurn) -> NexusPendingTurn:
    return NexusPendingTurn(
        turn_number=pt.turn_number, turn_id=pt.turn_id, message=pt.message
    )


def _nexus_subagent_info(info: SubagentInfo) -> NexusSubagentInfo:
    return NexusSubagentInfo(
        subagent_id=info.subagent_id,
        agent_key=info.agent_key,
        workflow_id=info.workflow_id,
        # The IDL requires this field but the harness tracks no per-subagent turn counter
        # (turn state is the child's own; query its agent_status). 0 is the explicit "not
        # tracked" value until the contract is regenerated with the rest of the Nexus
        # surface; no consumer reads it.
        next_expected_turn=0,
    )


def _nexus_approval_policy(policy: ToolApprovalPolicy) -> NexusApprovalPolicy:
    return NexusApprovalPolicy(
        dangerously_skip_all_approvals=policy.dangerously_skip_all_approvals,
        auto_approve_inherently_safe=policy.auto_approve_inherently_safe,
        auto_approve_tools=list(policy.auto_approve_tools),
    )


@dataclass(frozen=True)
class Config:
    """Per-deployment settings for AgentServiceHandler."""

    agent_task_queue: str
    workflow_name: str
    workflow_id_prefix: str


@service_handler(service=AgentServiceDefinition)
class AgentServiceHandler:
    """Exposes an agent session to external callers (e.g. the Slack connector).

    The connector calls ``sendAgentMessage`` to deliver user input and ``pollMessages`` to
    consume the agent's response stream. ``client`` carries the stream provider the agents'
    turn events are read through as a plugin.
    """

    def __init__(self, client: Client, config: Config) -> None:
        self._client = client
        self._config = config

    def _workflow_id(self, session_id: str) -> str:
        return self._config.workflow_id_prefix + session_id

    def _agent_client(self, session_id: str) -> AgentClient:
        """Cheap to construct per-call. Every operation but pollMessages delegates to it."""
        return AgentClient(self._client, self._workflow_id(session_id))

    # -----------------------------------------------------------------------
    # sendAgentMessage — AgentClient.start_and_submit_message()'s caller
    # -----------------------------------------------------------------------

    @sync_operation
    async def send_agent_message(
        self, ctx: StartOperationContext, input: SendAgentMessageInput
    ) -> SendMessageOutput:
        try:
            payload = json.loads(input.payload)
        except json.JSONDecodeError as e:
            raise HandlerError(
                f"invalid payload JSON: {e}", type=HandlerErrorType.BAD_REQUEST
            ) from e

        start_config = AgentConfig()
        client = self._agent_client(input.session_id)

        # The Nexus request id is the idempotency key: a retried operation re-issues the same
        # update and gets the original acceptance back rather than a second dispatch.
        reply = await client.start_and_submit_message(
            input.msg_type,
            payload,
            workflow_name=self._config.workflow_name,
            task_queue=self._config.agent_task_queue,
            start_config=start_config,
            update_id=f"send-{ctx.request_id}",
        )
        return SendMessageOutput(
            turn_number=reply.turn_number,
            turn_id=reply.turn_id,
            # The IDL still models acceptance as a bool; the harness now reports the
            # richer MessageDisposition (opened / joined / queued), of which "queued" is
            # exactly what this field meant. The contract regeneration that removes the
            # operator operations is where this becomes the disposition itself.
            pending=reply.disposition is MessageDisposition.QUEUED,
        )

    # -----------------------------------------------------------------------
    # executeOperatorCommand — REMOVED from the harness; see the body
    # -----------------------------------------------------------------------

    @sync_operation
    async def execute_operator_command(
        self, ctx: StartOperationContext, input: ExecuteOperatorCommandInput
    ) -> ExecuteOperatorCommandOutput:
        # NOT IMPLEMENTED — operator commands are no longer a harness concept. Every
        # control is an ordinary @agent.accepts handler reachable through
        # sendAgentMessage, and queryAgentInterface returns them all (with each
        # handler's midTurn + modelCallable). These two operations remain declared only
        # because the IDL still lists them and nexusrpc requires a complete service
        # handler; they are removed when the contract is regenerated.
        raise HandlerError(
            "operator commands have been removed from the harness — send the message to "
            "the target @agent.accepts handler via sendAgentMessage instead, and use "
            "queryAgentInterface to discover the available handlers.",
            type=HandlerErrorType.NOT_IMPLEMENTED,
        )

    # -----------------------------------------------------------------------
    # approveToolCall — resolve a pending tool-approval gate
    # -----------------------------------------------------------------------

    @sync_operation
    async def approve_tool_call(
        self, ctx: StartOperationContext, input: ApproveToolCallInput
    ) -> ApproveToolCallOutput:
        try:
            result = await self._agent_client(input.session_id).approve_tool(
                input.tool_id,
                approved=input.approved,
                reason=input.reason,
                remember=input.remember or False,
                update_id=f"approve-{ctx.request_id}",
            )
        except ToolApprovalError as e:
            raise HandlerError(str(e), type=HandlerErrorType.BAD_REQUEST) from e  # caller's fault
        return ApproveToolCallOutput(tool_id=result.tool_id, accepted=result.accepted)

    # -----------------------------------------------------------------------
    # queryOperatorInterface — REMOVED from the harness; see the body
    # -----------------------------------------------------------------------

    @sync_operation
    async def query_operator_interface(
        self, ctx: StartOperationContext, input: QuerySessionInput
    ) -> QueryOperatorInterfaceOutput:
        # NOT IMPLEMENTED — operator commands are no longer a harness concept. Every
        # control is an ordinary @agent.accepts handler reachable through
        # sendAgentMessage, and queryAgentInterface returns them all (with each
        # handler's midTurn + modelCallable). These two operations remain declared only
        # because the IDL still lists them and nexusrpc requires a complete service
        # handler; they are removed when the contract is regenerated.
        raise HandlerError(
            "operator commands have been removed from the harness — send the message to "
            "the target @agent.accepts handler via sendAgentMessage instead, and use "
            "queryAgentInterface to discover the available handlers.",
            type=HandlerErrorType.NOT_IMPLEMENTED,
        )

    # -----------------------------------------------------------------------
    # queryAgentInterface — discover @agent.accepts handlers
    # -----------------------------------------------------------------------

    @sync_operation
    async def query_agent_interface(
        self, ctx: StartOperationContext, input: QuerySessionInput
    ) -> AgentInterfaceOutput:
        functions = await self._agent_client(input.session_id).get_agent_interface()
        return AgentInterfaceOutput(
            handlers=[
                NexusAcceptedFunction(
                    name=fn.name,
                    description=fn.description,
                    parameters=json.dumps(fn.parameters),
                    output=json.dumps(fn.output),
                )
                for fn in functions
            ]
        )

    # -----------------------------------------------------------------------
    # queryAgentStatus — session state snapshot
    # -----------------------------------------------------------------------

    @sync_operation
    async def query_agent_status(
        self, ctx: StartOperationContext, input: QuerySessionInput
    ) -> AgentStatusOutput:
        status = await self._agent_client(input.session_id).get_status()
        return AgentStatusOutput(
            agent_id=status.agent_id,
            current_turn=status.current_turn,
            turn_active=status.turn_active,
            # The harness no longer has an agent-level queuing switch — mid-turn behavior
            # is declared per handler. The generated wire shape still requires this field,
            # so report False until the contract is regenerated without it.
            is_message_queuing_enabled=False,
            pending_turns=[_nexus_pending_turn(pt) for pt in status.pending_turns],
            pending_approvals=[
                NexusPendingApproval(
                    tool_id=pa.tool_id,
                    tool_name=pa.tool_name,
                    tool_input=json.dumps(pa.tool_input),
                    turn_number=pa.turn_number,
                )
                for pa in status.pending_approvals
            ],
            pending_callbacks=[
                _nexus_pending_callback(pc) for pc in status.pending_callbacks
            ],
            subagents=[_nexus_subagent_info(s) for s in status.subagents],
            approval_policy=_nexus_approval_policy(status.approval_policy),
            # The Nexus contract (agent.nexusrpc.yaml) is a CROSS-LANGUAGE wire contract
            # mirrored from remote and spoken by non-Python services, so its field keeps
            # its original name; the rename to "auto mode evaluator" is internal to
            # this package. Renaming it here would silently break every other consumer.
            has_custom_approval_fallback=status.has_auto_approval_evaluator,
        )

    # -----------------------------------------------------------------------
    # provideCallbackResult — fulfill a pending callback tool call
    # -----------------------------------------------------------------------

    @sync_operation
    async def provide_callback_result(
        self, ctx: StartOperationContext, input: ProvideCallbackResultInput
    ) -> ProvideCallbackResultOutput:
        # nex-gen wraps the object-shaped result in a named type instead of a plain dict.
        callback_result = (
            input.result.additional_properties if input.result is not None else None
        )
        try:
            result = await self._agent_client(input.session_id).provide_callback_result(
                input.tool_id,
                result=callback_result,
                error=input.error,
                update_id=f"callback-{ctx.request_id}",
            )
        except CallbackResultError as e:
            raise HandlerError(str(e), type=HandlerErrorType.BAD_REQUEST) from e  # caller's fault
        return ProvideCallbackResultOutput(
            tool_id=result.tool_id, accepted=result.accepted
        )

    # -----------------------------------------------------------------------
    # pollMessages — one batch of turn events after a cursor, through the stream interface
    # -----------------------------------------------------------------------

    @sync_operation
    async def poll_messages(
        self, ctx: StartOperationContext, input: PollMessagesInput
    ) -> PollMessagesOutput:
        """Wait up to the poll timeout for turn events after ``input.cursor`` and return them.

        ``cursor`` is the opaque token of the last item the caller handled (empty for the
        beginning); ``next_offset`` is the token to hand back next time. ``closed`` is set when
        the agent workflow has completed, which is when a stream that lives with the workflow
        ends; on a store that outlives it, the caller learns the same from the agent's status.
        """
        workflow_id = self._workflow_id(input.session_id)
        timeout = min(input.timeout_seconds or DEFAULT_POLL_TIMEOUT_SECONDS, MAX_POLL_SECONDS)
        converter = self._client.data_converter.payload_converter
        items: list[StreamItem] = []
        cursor = input.cursor
        closed = False
        more_ready = False
        try:
            # Opened inside the guard: the read parses the cursor at the call, so a token
            # this provider did not mint is the caller's mistake and not a handler failure.
            events = follow_turn_events(self._client, workflow_id, after=cursor)
        except (ValueError, StreamCursorError) as e:
            raise HandlerError(str(e), type=HandlerErrorType.BAD_REQUEST) from e
        try:
            async with asyncio.timeout(timeout) as window:
                async for record in events:
                    payload = converter.to_payloads([record.value])[0]
                    items.append(
                        StreamItem(
                            topic=TURN_EVENTS_TOPIC,
                            data=base64.b64encode(payload.SerializeToString()).decode("ascii"),
                            offset=record.cursor.token,
                        )
                    )
                    cursor = record.cursor.token
                    if len(items) >= POLL_BATCH_LIMIT:
                        more_ready = True
                        break
                    window.reschedule(
                        asyncio.get_running_loop().time() + POLL_BATCH_GRACE_SECONDS
                    )
                else:
                    # The subscription ended on its own. The shipped transport ends it when the
                    # workflow completes, but a transport may also end it quietly on the deadline,
                    # so the workflow itself is asked rather than the subscription trusted.
                    closed = await self._workflow_completed(workflow_id)
        except TimeoutError:
            pass
        except RPCError as e:
            if not _is_workflow_already_completed(e):
                raise
            closed = True
        finally:
            await events.aclose()
        return PollMessagesOutput(
            items=items, more_ready=more_ready, next_offset=cursor, closed=closed
        )

    async def _workflow_completed(self, workflow_id: str) -> bool:
        description = await self._client.get_workflow_handle(workflow_id).describe()
        return (
            description.status is not None
            and description.status != WorkflowExecutionStatus.RUNNING
        )
