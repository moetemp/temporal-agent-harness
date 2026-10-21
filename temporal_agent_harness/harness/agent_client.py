# ABOUTME: Stateless agent-turn client for agent workflows that publish through
# temporalio.contrib.streams.
#
# Abstracts a single agent turn — sending a user message to a Temporal workflow,
# streaming intermediate tool events, and terminating when the turn ends (the
# workflow's turn_end event) — into a single async iterator. Designed to be
# resumable via an opaque stream cursor so that disconnects don't lose events.

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from temporalio.client import Client, WithStartWorkflowOperation, WorkflowUpdateFailedError

from temporalio.common import WorkflowIDConflictPolicy

from temporal_agent_harness.harness.agent_protocol import (
    AGENT_INTERFACE_QUERY,
    AGENT_STATUS_QUERY,
    PROVIDE_CALLBACK_RESULT_UPDATE,
    SEND_AGENT_MESSAGE_UPDATE,
    SEND_SUBAGENT_MESSAGE_UPDATE,
    TOOL_APPROVAL_UPDATE,
    AcceptedFunction,
    AgentConfig,
    AgentEvent,
    AgentEventType,
    AgentMessage,
    AgentStatus,
    CallbackResult,
    MessageDisposition,
    CallbackResultAck,
    PendingApproval,
    PendingCallback,
    SubagentDispatch,
    SubagentMessage,
    ToolApprovalDecision,
    ToolApprovalResult,
    AgentMessageReply,
)
from temporal_agent_harness.harness.stream_transport import (
    check_turn_cursor,
    cursor_after,
    next_turn_cursor,
    read_turn_events,
    turn_stream,
)

# Client default: maximum seconds to wait for a turn to complete.
DEFAULT_TURN_TIMEOUT = 300.0

T = TypeVar("T")

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class AgentTurnTimeout(Exception):
    """Turn did not produce a terminal event before the timeout elapsed."""


class AgentTurnError(Exception):
    """Workflow published a terminal error event for the turn."""


class MidTurnRejectedError(Exception):
    """The target handler declares ``mid_turn=MidTurn.REJECT`` and a turn is already open.

    Retrying immediately will not help — the handler's author declared that this message
    must not pile up behind in-flight work. The caller should wait for the agent to go idle
    (``turn_end``, or ``agent_status`` reporting ``turn_active == False``) and send again, or
    send a different message whose handler declares ``ENQUEUE`` or ``ACCEPT``.
    """


class JoinedTurnError(Exception):
    """The message was admitted, but it JOINED an open turn — so it has no turn to stream.

    Raised by :meth:`AgentClient.send_message`, whose whole shape is "one message, one turn,
    one stream". A ``MidTurn.ACCEPT`` message sent to a busy agent joins the turn already
    running: its ``turn_started`` is *behind* the position read before sending, so the reader's
    skip preamble would never match and the caller would sit in silence until the turn timeout.
    Failing here instead makes that a fast, legible error.

    **The message is running.** This is not a rejection and there is nothing to retry: the
    agent accepted it, the handler is executing, and its reply will be published on the stream
    under :attr:`reply`'s ``message_id``. Only the streaming half is unavailable. A caller that
    needs to observe it uses the documented client contract instead — :meth:`submit_message`
    for every send, one :meth:`attach` stream kept open — which is what an interactive client
    should be doing anyway (see the README, "The client contract").
    """

    def __init__(self, message: str, *, reply: AgentMessageReply) -> None:
        super().__init__(message)
        self.reply = reply
        """The accepted reply — ``message_id`` / ``turn_number`` / ``turn_id`` for the message
        that is now running, so the caller can find it on a stream it opens itself."""


class ToolApprovalError(Exception):
    """A tool-approval decision was rejected by the workflow.

    Raised by :meth:`AgentClient.approve_tool` when the workflow's update validator
    rejects the decision — the ``tool_id`` is unknown, or the approval was already
    resolved (a double-submit). ``error_type`` carries the workflow-side type
    (``UnknownToolApproval`` / ``ToolApprovalAlreadyResolved``).
    """

    def __init__(self, message: str, *, error_type: str | None = None) -> None:
        super().__init__(message)
        self.error_type = error_type


class CallbackResultError(Exception):
    """A callback tool result was rejected by the workflow.

    Raised by :meth:`AgentClient.provide_callback_result` when the workflow's update validator
    rejects the submission — the ``tool_id`` is unknown, the callback was already resolved (a
    double-submit), or the result payload does not match the tool's declared output type.
    ``error_type`` carries the workflow-side type (``UnknownCallback`` /
    ``CallbackAlreadyResolved`` / ``MalformedCallbackResult``).
    """

    def __init__(self, message: str, *, error_type: str | None = None) -> None:
        super().__init__(message)
        self.error_type = error_type


# Must be defined after the exception classes since it references them at runtime.
AgentStreamOutput = AgentEvent | AgentTurnError | AgentTurnTimeout



@dataclass(frozen=True)
class ResumePoint:
    """Where a consumer of an agent's stream left off.

    ``cursor`` is the Stream position just past the last event given (empty for the
    beginning). Handing it back as ``attach(resume=...)`` resumes there. ``seq`` is the agent's
    own count of workflow-published events as of that point (``AgentEvent.seq``), which is how a
    consumer tells that it has caught up with everything the agent has said
    (``AgentStatus.last_event_seq``), since a Stream position cannot answer that. The pair
    encodes to one string for a UI or a Nexus caller to store.
    """

    cursor: str = ""
    seq: int = 0

    def advance(self, *, cursor: str, seq: int | None) -> ResumePoint:
        """The point just past a record; a record without a seq of this agent keeps the last."""
        return ResumePoint(cursor=cursor, seq=self.seq if seq is None else seq)

    def encode(self) -> str:
        """One string for a consumer to store and hand back."""
        if not (self.cursor or self.seq):
            return ""
        return f"{self.seq}@{self.cursor}"

    @classmethod
    def decode(cls, text: str) -> ResumePoint:
        """The point ``text`` encodes. Raises ``ValueError`` when the text is malformed."""
        if not text:
            return cls()
        seq, sep, cursor = text.partition("@")
        if not sep:
            raise ValueError(f"not a resume point: {text!r}")
        try:
            return cls(cursor=cursor, seq=int(seq))
        except ValueError:
            raise ValueError(f"not a resume point: {text!r}") from None


# The callback signature: (item, resume) -> T
# ``resume`` is the resume point as of this item — a value the consumer records
# (``resume.encode()``) and hands back as ``attach(resume=...)`` to resume.
OnItemCallback = Callable[[AgentStreamOutput, ResumePoint], T]


# ---------------------------------------------------------------------------
# AgentClient
# ---------------------------------------------------------------------------


class AgentClient:
    """Stateless client for interacting with an agent workflow.

    All durable state lives in the workflow (turn counter, liveness,
    stream log). This client is cheap to construct per-request.

    Args:
        temporal: Connected Temporal client, with the stream store the agent's turn
            events are read through registered on it as a plugin.
        workflow_id: ID of the agent workflow to interact with.
    """

    def __init__(
        self,
        temporal: Client,
        workflow_id: str,
    ) -> None:
        self._temporal = temporal
        self._workflow_id = workflow_id

    @property
    def workflow_id(self) -> str:
        return self._workflow_id

    async def get_status(self) -> AgentStatus:
        """Query the workflow for its current status."""
        handle = self._temporal.get_workflow_handle(self._workflow_id)
        return await handle.query(AGENT_STATUS_QUERY, result_type=AgentStatus)

    async def get_pending_approvals(self) -> list[PendingApproval]:
        """The gated tool calls currently awaiting a human decision.

        A convenience over :meth:`get_status` — lets a client that attached after the
        ``tool_approval_requested`` event was published still discover and act on
        outstanding approvals (the event may have streamed by before it connected).
        """
        status = await self.get_status()
        return status.pending_approvals

    async def approve_tool(
        self,
        tool_id: str,
        *,
        approved: bool,
        reason: str | None = None,
        remember: bool = False,
        update_id: str | None = None,
    ) -> ToolApprovalResult:
        """Resolve a pending tool approval (see :class:`ToolApprovalRequested`).

        ``tool_id`` is the id carried by the ``tool_approval_requested`` event and listed
        under :attr:`AgentStatus.pending_approvals`. On approval the gated call dispatches
        immediately; on denial the model receives ``reason`` as the tool's error result.
        Resolving an unknown id, or one already resolved, raises :class:`ToolApprovalError`
        (the decision is idempotent — a double-submit fails rather than flipping it).

        ``remember=True`` (only meaningful with ``approved=True``) is "approve, and stop
        asking me about this tool": it adds the tool to the agent's live
        :class:`ToolApprovalPolicy` allow-list, so future calls of it skip the gate and any
        other call of that tool currently waiting auto-resolves. Read the updated policy
        back via :meth:`get_status` (``approval_policy``) to persist it for next session.

        ``update_id`` lets a caller supply its own idempotency key (e.g. a retried Nexus
        request) instead of an auto-generated one, so retries resolve the same update
        instead of double-submitting.
        """
        handle = self._temporal.get_workflow_handle(self._workflow_id)
        try:
            return await handle.execute_update(
                TOOL_APPROVAL_UPDATE,
                ToolApprovalDecision(
                    tool_id=tool_id, approved=approved, reason=reason, remember=remember
                ),
                id=update_id,
                result_type=ToolApprovalResult,
            )
        except WorkflowUpdateFailedError as e:
            cause = e.cause
            error_type = getattr(cause, "type", None) if cause else None
            if error_type in ("UnknownToolApproval", "ToolApprovalAlreadyResolved"):
                raise ToolApprovalError(str(cause), error_type=error_type) from e
            raise

    async def get_pending_callbacks(self) -> list[PendingCallback]:
        """The callback tool calls currently awaiting a client-supplied result.

        A convenience over :meth:`get_status` — lets a client that attached after the
        ``callback_requested`` event was published still discover and fulfill outstanding
        callback calls (each carries the ``output_schema`` the result must match).
        """
        status = await self.get_status()
        return status.pending_callbacks

    async def provide_callback_result(
        self,
        tool_id: str,
        *,
        result: Any = None,
        error: str | None = None,
        update_id: str | None = None,
    ) -> CallbackResultAck:
        """Fulfill a pending callback tool call (see :class:`CallbackRequested`).

        A callback tool has no worker-side body — the agent paused in-workflow while THIS client
        executed the tool on its own machine (e.g. read a local file, captured a photo). Submit
        the outcome here, keyed by the ``tool_id`` from the ``callback_requested`` event (also
        listed under :attr:`AgentStatus.pending_callbacks`):

          * ``result`` — the value produced, as JSON-native data (a dict for a pydantic-model
            output, or a scalar/list). It is validated against the tool's declared output type
            and becomes the tool's return value.
          * ``error`` — set instead when the tool could not be fulfilled; the model receives it
            as the tool's error result rather than the turn crashing.

        Resolving an unknown id, one already resolved, or a result that fails output-type
        validation raises :class:`CallbackResultError` (idempotent — a double-submit fails
        rather than overwriting a settled result; a malformed result can be corrected and
        resubmitted, since it does not consume the pending gate).

        ``update_id`` — see :meth:`approve_tool`'s note on caller-supplied idempotency keys.
        """
        handle = self._temporal.get_workflow_handle(self._workflow_id)
        try:
            return await handle.execute_update(
                PROVIDE_CALLBACK_RESULT_UPDATE,
                CallbackResult(tool_id=tool_id, result=result, error=error),
                id=update_id,
                result_type=CallbackResultAck,
            )
        except WorkflowUpdateFailedError as e:
            cause = e.cause
            error_type = getattr(cause, "type", None) if cause else None
            if error_type in (
                "UnknownCallback",
                "CallbackAlreadyResolved",
                "MalformedCallbackResult",
            ):
                raise CallbackResultError(str(cause), error_type=error_type) from e
            raise

    async def get_agent_interface(self) -> list[AcceptedFunction]:
        """Query the workflow for its callable surface — the ``@agent.accepts`` handlers.

        Returns one :class:`AcceptedFunction` per handler (``name`` / ``description`` /
        input ``parameters`` schema / ``output`` schema), Gemini-tool-shaped. Callers
        introspect this to construct a valid :class:`AgentMessage` for any handler (the
        ``name`` is the envelope ``type``), or — for a parent agent — to model each handler
        as a subagent tool, so the contract can evolve without client-side changes.
        """
        handle = self._temporal.get_workflow_handle(self._workflow_id)
        return await handle.query(AGENT_INTERFACE_QUERY, result_type=list[AcceptedFunction])

    async def _submit_message(
        self,
        msg_type: str,
        payload: dict[str, Any],
        *,
        update_id: str | None = None,
        dispatch: SubagentDispatch | None = None,
    ) -> AgentMessageReply:
        """Submit one message to the agent's front door, WITHOUT streaming the turn.

        Internal harness primitive — phase 1 of a turn. Public callers use :meth:`send_message`
        (the send-and-stream convenience); this and :meth:`_stream_turn` are the composable
        halves it is built from, reused in-package (e.g. by the subagent-turn activity, which
        sends then streams with its own dedup/heartbeat in between). Names the target
        ``@agent.accepts`` handler (``msg_type``) and carries its input-model JSON
        (``payload``), builds the ``AgentMessage`` envelope internally, and forwards it as the
        ``send_agent_message`` update. Returns the accepted :class:`AgentMessageReply`
        (``message_id`` / ``turn_id`` / ``turn_number`` / ``disposition``).

        ``update_id`` is for retrying callers (e.g. the harness's own ``run_subagent_turn``
        activity), which key it on an identity stable across their retries so a re-issued send
        after a lost reply gets the original acceptance back rather than dispatching a second
        message. Left ``None``, Temporal mints one.

        ``dispatch`` is for the subagent-turn activity: the message goes as the internal
        ``send_subagent_message`` update, and the child announces it on the parent's Topic.

        Raises:
            MidTurnRejectedError: The handler refuses mid-turn arrival and a turn is open.
        """
        handle = self._temporal.get_workflow_handle(self._workflow_id)
        message = AgentMessage(type=msg_type, payload=payload)
        try:
            if dispatch is not None:
                return await handle.execute_update(
                    SEND_SUBAGENT_MESSAGE_UPDATE,
                    SubagentMessage(message=message, dispatch=dispatch),
                    id=update_id,
                    result_type=AgentMessageReply,
                )
            return await handle.execute_update(
                SEND_AGENT_MESSAGE_UPDATE,
                message,
                id=update_id,
                result_type=AgentMessageReply,
            )
        except WorkflowUpdateFailedError as e:
            cause = e.cause
            error_type = getattr(cause, "type", None) if cause else None
            if error_type == "MidTurnRejected":
                raise MidTurnRejectedError(str(cause)) from e
            raise

    async def submit_message(
        self,
        msg_type: str,
        payload: dict[str, Any],
        *,
        update_id: str | None = None,
    ) -> AgentMessageReply:
        """Submit one message to the agent without streaming the accepted turn.

        This is the send half of the client contract: an interactive client sends EVERY message
        this way and keeps at most one :meth:`attach` stream, ensured live *after* the send
        returns (see the README, "The client contract"). Doing it the other way
        round — one :meth:`send_message` stream per message — opens a long-lived stream per
        queued message and cannot observe a joined one at all.

        The returned :class:`AgentMessageReply` carries the message's ``message_id`` (which
        every event of its dispatch is stamped with), the ``turn_number`` / ``turn_id`` it
        belongs to, and the ``disposition`` saying whether it opened, joined or queued behind
        a turn.

        ``update_id`` is an idempotency key for a caller whose delivery can be repeated — a
        chat-platform webhook is redelivered routinely — keyed on an identity stable across
        those repeats (the platform's own event id). A re-issued send then gets the original
        acceptance back instead of dispatching the message a second time. Left ``None``,
        Temporal mints one and every delivery is a fresh dispatch.
        """
        return await self._submit_message(msg_type, payload, update_id=update_id)

    async def start_and_submit_message(
        self,
        msg_type: str,
        payload: dict[str, Any],
        *,
        workflow_name: str,
        task_queue: str,
        start_config: AgentConfig,
        update_id: str | None = None,
    ) -> AgentMessageReply:
        """Like :meth:`_submit_message`, but starts the workflow first if it isn't already
        running — for callers that can't assume it exists (e.g. Nexus's
        ``sendAgentMessage``, which must create-or-reuse a session in one call). Needs
        ``workflow_name``/``task_queue`` since every other method assumes the workflow is
        already running and never needs them.

        ``update_id`` — see :meth:`approve_tool`'s note.

        Raises:
            MidTurnRejectedError: The handler refuses mid-turn arrival and a turn is open.
        """
        start_op = WithStartWorkflowOperation(
            workflow_name,
            start_config,
            id=self._workflow_id,
            task_queue=task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        try:
            return await self._temporal.execute_update_with_start_workflow(
                SEND_AGENT_MESSAGE_UPDATE,
                AgentMessage(type=msg_type, payload=payload),
                start_workflow_operation=start_op,
                id=update_id,
                result_type=AgentMessageReply,
            )
        except WorkflowUpdateFailedError as e:
            cause = e.cause
            error_type = getattr(cause, "type", None) if cause else None
            if error_type == "MidTurnRejected":
                raise MidTurnRejectedError(str(cause)) from e
            raise

    async def send_message(
        self,
        msg_type: str,
        payload: dict[str, Any],
        *,
        on_item: OnItemCallback[T],
        timeout: float | None = DEFAULT_TURN_TIMEOUT,
        update_id: str | None = None,
    ) -> AsyncIterator[T]:
        """Send a message and stream the resulting turn — including any subagents — as ONE stream.

        PRECONDITION — this is for a caller that holds NO stream, sending a message that gets a
        turn OF ITS OWN (a script or connector doing one request/response). It is NOT the mid-turn
        surface. A ``MidTurn.ACCEPT`` message that JOINS an open turn has that turn's
        ``turn_started`` *behind* the position read before sending, so the reader's skip preamble
        would never match and nothing would ever be emitted. The reply's ``disposition`` says which
        happened, so that case FAILS FAST with :class:`JoinedTurnError` (carrying the accepted
        reply — the message is running regardless) rather than going silent for ``timeout``
        seconds. An interactive client sends every message with :meth:`submit_message` and keeps
        ONE :meth:`attach` stream open instead — see the README, "The client contract".

        Phase 1, :meth:`_submit_message`, runs eagerly here so ``MidTurnRejectedError`` is raised
        *before* any streaming begins. The client reads the Stream's next position before
        submitting, so the turn's first record cannot slip past it; phase 2 then reads the tree's
        shared Stream from there: it skips to this turn's ``turn_started`` (a quiescent start) and
        yields every event of the turn, the agent's own and each subagent's below it, in the
        Stream's one order — through to this turn's ``turn_end``. The caller never tracks or
        passes a position.

        Args:
            msg_type: Name of the target ``@agent.accepts`` handler.
            payload: JSON of that handler's input model.
            on_item: Callback ``(AgentStreamOutput, resume) -> T`` applied to each output.
                ``resume`` is the resume point as of this item (see :meth:`attach`); the
                per-turn path doesn't resume on it (the chat path reattaches via
                :meth:`attach`), but it is passed through uniformly so a caller's bookkeeping is
                consistent across both entry points.
            timeout: Max seconds to wait for the turn to complete (``None`` = no limit).
            update_id: Idempotency key for the send, for a caller whose delivery can be
                repeated (a redelivered chat-platform webhook). A repeat gets the original
                acceptance back — and therefore streams the ORIGINAL turn — rather than
                dispatching the message twice. See :meth:`submit_message`.

        Returns:
            An async iterator of ``T``.

        Raises:
            MidTurnRejectedError: The handler refuses mid-turn arrival and a turn is open.
            JoinedTurnError: The message was accepted, but it joined an open turn — it is
                running, and there is no per-turn stream for it (see the precondition above).
        """
        # Positioned before submitting, so the turn's first record cannot land before the reader
        # is looking. The workflow does not know where its records land, so the client asks the
        # Stream rather than the agent.
        since = await next_turn_cursor(self._temporal, self._workflow_id)
        reply = await self._submit_message(msg_type, payload, update_id=update_id)
        if reply.disposition is MessageDisposition.JOINED:
            raise JoinedTurnError(
                f"message {reply.message_id} joined open turn {reply.turn_number} and is "
                f"running; send_message cannot stream a joined turn — submit_message + "
                f"attach is the mid-turn surface",
                reply=reply,
            )
        return self._turn(reply, since=since, on_item=on_item, timeout=timeout)

    async def _turn(
        self,
        reply: AgentMessageReply,
        *,
        since: str,
        on_item: OnItemCallback[T],
        timeout: float | None,
    ) -> AsyncIterator[T]:
        """Phase 2 of :meth:`send_message`: read the tree's Stream for one submitted turn.

        Reads from ``since`` (the position captured before submitting) and skips to
        ``reply.turn_id``'s ``turn_started``, then follows until that turn's ``turn_end`` on this
        agent — by which point every subagent turn it triggered has already been emitted, since
        the agent's ``subagent_reply_received`` waits on the subagent's ``turn_end``. THIS
        MESSAGE's ``message_handler_error`` is surfaced as an :class:`AgentTurnError` — a sibling
        participant's is not, since it says nothing about the caller's own message; on timeout
        an :class:`AgentTurnTimeout` is yielded last.
        """
        target_turn_id = reply.turn_id
        target_message_id = reply.message_id

        async def should_stop(own: bool, ev: AgentEvent) -> bool:
            return (
                own and ev.turn_id == target_turn_id and ev.event.type == AgentEventType.TURN_END
            )

        events = self._follow_tree(
            resume=ResumePoint(cursor=since),
            skip_until_turn_id=target_turn_id,
            should_stop=should_stop,
        )
        try:
            async with asyncio.timeout(timeout):
                async for ev, resume in events:
                    # Match on OUR MESSAGE, not our turn: a ``MidTurn.ACCEPT`` message can join
                    # the turn we opened, and its failure is not ours to raise. ``message_id`` is
                    # a globally-unique uuid, so it alone identifies our message's terminal error
                    # — no need to also match turn_id or agent_id.
                    if (
                        ev.message_id == target_message_id
                        and ev.event.type == AgentEventType.MESSAGE_HANDLER_ERROR
                    ):
                        # Surface OUR message's error as the caller's failure signal in place of
                        # the raw event (turn_end still follows as the real terminal).
                        yield on_item(
                            AgentTurnError(ev.event.message or "agent turn failed"),
                            resume,
                        )
                    else:
                        yield on_item(ev, resume)
        except TimeoutError:
            yield on_item(
                AgentTurnTimeout(f"turn {reply.turn_number} did not complete within {timeout}s"),
                ResumePoint(),
            )
        finally:
            await events.aclose()

    async def attach(
        self,
        *,
        on_item: OnItemCallback[T],
        resume: str = "",
    ) -> AsyncIterator[T]:
        """Reattach to a session and stream it — subagents included — as ONE stream, then tail.

        ``resume`` controls where the read starts:

        * **empty (default)** — full replay from the beginning, for a blank-slate consumer (a
          freshly loaded tab) that has no prior state. Replays past events, then follows live
          until the agent is idle.
        * **an encoded resume point** — resume: stream from just past that record onward, so
          already-seen events are not re-sent.

        ``on_item`` receives ``(item, resume)``. Record the latest ``resume.encode()`` and pass it
        back to resume. Every agent of the tree publishes on one Stream, so the point advances on
        every event, the subagents' too, and a resume loses nothing: a consumer that disconnects
        in the middle of a subagent's turn gets the rest of that turn on resume.

        Returns immediately (yields nothing) if there's nothing new to stream.

        Termination mirrors the per-turn close: on each ``turn_end`` of this agent we re-query
        status and stop once the workflow is idle and the event just emitted is the last one the
        agent published (``AgentEvent.seq`` against ``AgentStatus.last_event_seq``). ``turn_end``
        is the only terminal there is — it fires when the last participant of a turn finishes,
        so it is a true quiescence signal — which is what lets a caller safely disconnect instead
        of holding the stream open forever. The agent's own count is what makes this
        store-independent: a stream position could not say whether anything followed.

        A malformed ``resume`` raises ``ValueError``, and one that names no position on the
        agent's Stream raises ``TurnCursorError`` (a ``ValueError`` too), both before anything
        streams, so a caller can answer with a client error and start over.
        """
        point = ResumePoint.decode(resume)
        # Checked here, before the response starts, rather than at the read's first step.
        await check_turn_cursor(self._temporal, self._workflow_id, point.cursor)
        status = await self.get_status()
        # Already caught up (the point is at or past the agent's last word) and the agent is
        # idle — nothing to stream.
        if (
            not status.turn_active
            and not status.pending_turns
            and point.seq >= status.last_event_seq
        ):
            return self._empty()
        return self._attached(on_item=on_item, resume=point)

    async def _empty(self) -> AsyncIterator[T]:
        """An async iterator that yields nothing (the no-history, idle ``attach`` case)."""
        return
        yield  # pragma: no cover — unreachable; makes this an async generator

    async def _attached(
        self,
        *,
        on_item: OnItemCallback[T],
        resume: ResumePoint,
    ) -> AsyncIterator[T]:
        """Phase 2 of :meth:`attach`: read from ``resume`` to the next idle point, no skip."""

        async def should_stop(own: bool, ev: AgentEvent) -> bool:
            if not own:
                return False
            # Every terminal is a turn_end now: a turn is the interval the agent is
            # non-idle, so there is no out-of-band action that completes without one.
            #
            # State registration is the one exception, because it publishes BEFORE any
            # turn exists: declared state is registered when the runner is built in
            # `@workflow.init`, so its snapshot lands at the very front of the stream stamped
            # `turn_number=0`,
            # which is why it is matched on that rather than on being a snapshot. It is
            # the only event a replay can end on that no `turn_end` will ever follow, so
            # it has to be able to end one. Without this, attach delivered a brand-new
            # session's one-event history and then blocked forever -- and in the console
            # that closed a loop, because `creatingSession` is cleared when the first
            # attach goes idle and it gates the composer, so the session just created
            # could not be sent the message that would have ended the attach. (An agent
            # registering several states puts one of these at each of its first n events; the
            # seq check below is what keeps all but the last from ending the replay early.)
            registration_snapshot = (
                ev.event.type == AgentEventType.STATE_SNAPSHOT and ev.turn_number == 0
            )
            if ev.event.type != AgentEventType.TURN_END and not registration_snapshot:
                return False
            try:
                status = await self.get_status()
            except Exception:  # noqa: BLE001 — workflow gone (e.g. failed after an errored turn)
                return True
            # Caught up when this terminal event is the last thing the agent published and it
            # is idle. An activity-published event carries no seq and can never be the last word.
            return (
                not status.turn_active
                and not status.pending_turns
                and ev.seq is not None
                and ev.seq >= status.last_event_seq
            )

        events = self._follow_tree(
            resume=resume, skip_until_turn_id=None, should_stop=should_stop
        )
        try:
            async for ev, point in events:
                yield on_item(ev, point)
        finally:
            await events.aclose()

    async def _follow_tree(
        self,
        *,
        resume: ResumePoint,
        skip_until_turn_id: str | None,
        should_stop: Callable[[bool, AgentEvent], Awaitable[bool]],
    ) -> AsyncGenerator[tuple[AgentEvent, ResumePoint], None]:
        """Yield ``(event, resume point)`` for this agent and every subagent below it.

        The whole tree publishes on one Stream, so the Stream's order is the order: a subagent's
        events come after the ``subagent_message_sent`` that asked for them (the child writes
        that marker before its own events) and before the ``subagent_reply_received`` (the
        parent writes it after it read the child's ``turn_end``). ``should_stop`` gets whether
        the event is this agent's own, and ends the read when it returns True.
        """
        located = await turn_stream(self._temporal, self._workflow_id)
        records = read_turn_events(located, after=resume.cursor, subtree=True)
        point = resume
        skipping = skip_until_turn_id is not None
        try:
            async for record in records:
                ev = record.data
                own = record.topic == located.topic
                if skipping:
                    # A quiescent start: the first event of the turn is this agent's own
                    # ``turn_started``, and nothing of the turn comes before it.
                    if not (
                        own
                        and ev.turn_id == skip_until_turn_id
                        and ev.event.type == AgentEventType.TURN_STARTED
                    ):
                        point = point.advance(cursor=cursor_after(record), seq=None)
                        continue
                    skipping = False
                point = point.advance(cursor=cursor_after(record), seq=ev.seq if own else None)
                yield ev, point
                if await should_stop(own, ev):
                    return
        finally:
            await records.aclose()
