# ABOUTME: The harness subagent-turn activity — one activity that drives a single turn of a
# CHILD agent workflow: it sends the message to the child and waits for the child's result,
# returning the reply payload. One activity call per subagent turn (cleaner than a send/wait
# split), with heartbeat state used as an "already sent?" memo so the common retry (a crash
# while waiting) resumes *waiting* instead of re-sending.
#
# DESIGN. No wait timeout + interval auto-heartbeat: we NEVER cap how long we wait for the
# subagent's reply. A subagent may legitimately take arbitrarily long, so a background task
# heartbeats at a STEADY interval (derived as ``heartbeat_timeout / 2``, mirroring
# temporalio.contrib.openai_agents' auto-heartbeater) carrying the latest dedup memo. The
# activity's liveness is therefore the ``heartbeat_timeout`` (a short, predictable grace
# window — Temporal reaps a dead worker fast), NOT a guess at how long the turn "should" take.
# The activity runs until its ``start_to_close_timeout`` ceiling (set by the caller — Temporal
# requires one of the close timeouts; the toolset generator uses a generous default that devs
# can override).
#
# DESIGN. The reply comes from the child's ``await_message`` update, NOT from its events. The
# event stream is a live view, on either backend, and a parent's model must get its subagent's
# answer even when a view misses an event. The activity mirrors none of the child's events onto
# the parent's. On Workflow Streams it publishes ONE marker of its own onto the PARENT's stream
# (the SubagentMessageSent dispatch event, when it actually sends the message; see
# _publish_dispatch). On ``temporalio.common.streams`` the child publishes that marker, so it
# comes before the child's events on the one Stream they share.
#
# DESIGN. Temporal Client: the activity needs a ``Client`` to talk to the *child* (the
# ``send_subagent_message`` and ``await_message`` updates). An activity's own context only
# reaches its parent workflow. So this is a CLASS that closes over the worker's client; the bound
# method is what gets registered as the activity. ``AgentHarnessPlugin`` does that instantiation
# from the worker's own client (it is why the plugin overrides ``configure_worker``), so agents
# normally never touch this class. Reach for it directly only when hand-building a worker's
# activity list (``activities=[SubagentActivities(client).run_subagent_turn]``).

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pydantic import BaseModel
from temporalio import activity
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError, RPCStatusCode

from temporal_agent_harness.harness.agent_client import (
    MidTurnRejectedError,
    AgentClient,
)
from temporal_agent_harness.harness.agent_protocol import (
    AWAIT_MESSAGE_UPDATE,
    DEFAULT_SUBAGENT_HEARTBEAT_TIMEOUT,
    RUN_SUBAGENT_TURN_ACTIVITY,
    MessageResult,
    RunSubagentTurnInput,
    SubagentDispatch,
    SubagentMessageSent,
    SubagentTurnResult,
)
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner


class _TurnProgress(BaseModel):
    """The activity's heartbeat memo — what a retry needs to resume without re-sending.

    Recorded once the message has been sent (``sent`` is always True when present), carrying
    the child's accepted ``message_id`` / ``turn_id`` / ``turn_number``. Its presence in
    ``heartbeat_details`` is the "already sent?" signal; the background heartbeat task re-sends
    THIS object every interval, so it is never clobbered by an empty heartbeat.

    ``message_id`` is what a resumed attempt waits on with ``await_message``.
    """

    sent: bool
    turn_id: str
    turn_number: int
    message_id: str


class SubagentActivities:
    """Harness activities for driving subagents, bound to a Temporal :class:`Client`.

    ``AgentHarnessPlugin`` constructs this from the worker's own client and registers the
    bound activity method, so agents normally never name this class. Do it by hand only when
    assembling a worker's activity list yourself::

        subagents = SubagentActivities(client)
        Worker(..., activities=[subagents.run_subagent_turn, ...])

    Kept a class (rather than a module-level client global) so the client is an explicit
    construction dependency — which is what lets the plugin supply the worker's own.
    """

    def __init__(self, client: Client) -> None:
        self._client = client

    @activity.defn(name=RUN_SUBAGENT_TURN_ACTIVITY)
    async def run_subagent_turn(self, req: RunSubagentTurnInput) -> SubagentTurnResult:
        """Send one message to the child agent and wait for its result.

        Sends the ``send_subagent_message`` envelope to ``req.child_workflow_id`` (unless a
        heartbeat memo says a prior attempt already sent it), then waits, with NO timeout,
        on the child's ``await_message`` update for that message. A background task heartbeats
        the dedup memo at a steady interval throughout (see :meth:`_auto_heartbeat`).

        Failure modes surface as non-retryable :class:`ApplicationError` so the calling tool
        can render them as an ``is_error`` result to the parent model:

        * the child rejected the send (``MidTurnRejected`` / ``UnknownFunction`` /
          ``MalformedMessage``) — the child's error ``type`` is preserved;
        * the child's handler raised (``SubagentTurnError``);
        * the child ended before the message did, such as a failed or terminated child
          (``SubagentNoReply``).
        """
        client = AgentClient(self._client, req.child_workflow_id)

        # "Already sent?" memo: a retry that landed after the send resumes waiting instead of
        # re-submitting the turn. The send itself is also idempotent (``_submit`` keys the
        # update on this activity's identity, stable across attempts), so a crash between the
        # update returning and the first heartbeat being durably recorded re-issues the update
        # and gets the same accepted reply back rather than dispatching the message twice. On
        # Workflow Streams what that window can still duplicate is the
        # ``subagent_message_sent`` marker below, which only the memo dedupes.
        progress = self._resume_progress()
        if progress is None:
            progress = await self._submit(client, req)
            if req.parent_stream_context.stream is None:
                # The send just happened, so NOW publish the dispatch marker onto the PARENT's
                # stream (this is the accurate moment, vs. the parent's execute_activity
                # dispatch time). Only on the fresh-send branch: a heartbeat-resume retry skips
                # both the re-send and this publish.
                await self._publish_dispatch(req, progress)
        # Record/refresh the memo immediately (covers both the fresh-send and resume paths),
        # then let the background task keep it alive at a steady cadence.
        activity.heartbeat(progress)

        async with self._auto_heartbeat(progress):
            result = await self._await_result(req, progress)

        if result.error is not None:
            # Carry the child's ACTUAL accepted turn number in the details: the parent closes
            # the [message_sent … reply_received] bracket on this exact turn number (the same
            # one the dispatch marker opened with), never a re-derived `expected` (see
            # AgentWorkflowRunner._accepted_turn_from_error).
            raise ApplicationError(
                result.error or "subagent turn failed",
                {"subagent_turn": progress.turn_number},
                type="SubagentTurnError",
                non_retryable=True,
            )
        return SubagentTurnResult(
            output=result.output or {},
            turn_id=progress.turn_id,
            turn_number=progress.turn_number,
            consumed_offset=result.stream_offset,
        )

    async def _await_result(
        self, req: RunSubagentTurnInput, progress: _TurnProgress
    ) -> MessageResult:
        """The child's result for our message, from its ``await_message`` update.

        The update id is this activity's identity. A retry after the child answered gets the
        same answer back from Temporal, though the child dropped the result once it gave it.
        """
        info = activity.info()
        handle = self._client.get_workflow_handle(req.child_workflow_id)
        try:
            return await handle.execute_update(
                AWAIT_MESSAGE_UPDATE,
                progress.message_id,
                id=f"subagent-await:{info.workflow_run_id}:{info.activity_id}",
                result_type=MessageResult,
            )
        except WorkflowUpdateFailedError as e:
            reason = str(e.cause) if e.cause else "the subagent gave no result"
        except RPCError as e:
            # A child that is gone will never answer. Any other RPC error may pass, so the
            # attempt fails and Temporal retries it.
            if e.status != RPCStatusCode.NOT_FOUND:
                raise
            reason = str(e)
        raise ApplicationError(
            f"subagent turn {progress.turn_number} ended without a reply: {reason}",
            {"subagent_turn": progress.turn_number},
            type="SubagentNoReply",
            non_retryable=True,
        )

    @asynccontextmanager
    async def _auto_heartbeat(self, progress: _TurnProgress) -> AsyncIterator[None]:
        """Heartbeat ``progress`` at a steady interval for the duration of the block.

        Mirrors ``temporalio.contrib.openai_agents``' auto-heartbeater: it heartbeats every
        ``heartbeat_timeout / 2`` so liveness is predictable however long the subagent takes
        (the activity is never mistaken for dead just because the turn is slow). It always
        heartbeats ``progress``, never an empty heartbeat, so the "already sent?" memo is never
        clobbered. Falls back to a fixed interval if no ``heartbeat_timeout`` was configured
        (heartbeating is harmless).
        """
        # Heartbeat at half the configured timeout; if the caller configured none, fall back
        # to the harness default (the wrapper always sets one, so this is just insurance).
        heartbeat_timeout = (
            activity.info().heartbeat_timeout or DEFAULT_SUBAGENT_HEARTBEAT_TIMEOUT
        )
        interval = heartbeat_timeout.total_seconds() / 2

        async def beat() -> None:
            while True:
                await asyncio.sleep(interval)
                activity.heartbeat(progress)

        task = asyncio.create_task(beat())
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _submit(
        self, client: AgentClient, req: RunSubagentTurnInput
    ) -> _TurnProgress:
        """Submit the message to the child, and build the resume memo.

        Delegates the envelope build + update to :meth:`AgentClient._submit_message`, then
        translates a rejection into a non-retryable :class:`ApplicationError` that preserves
        the child's error ``type`` (``MidTurnRejected`` / ``UnknownFunction`` /
        ``MalformedMessage``), so the calling tool can surface it verbatim.

        The update is keyed on THIS activity's identity — the parent run + activity id, which
        every retry attempt shares — so a re-issued send after a lost reply is deduplicated by
        Temporal and returns the original acceptance instead of dispatching the child a second
        message.
        """
        info = activity.info()
        update_id = f"subagent-turn:{info.workflow_run_id}:{info.activity_id}"
        try:
            result = await client._submit_message(
                req.type,
                req.payload,
                update_id=update_id,
                dispatch=SubagentDispatch(
                    agent_key=req.agent_key, parent_stream_context=req.parent_stream_context
                ),
            )
        except MidTurnRejectedError as e:
            raise ApplicationError(
                str(e), type="MidTurnRejected", non_retryable=True
            ) from e
        except WorkflowUpdateFailedError as e:
            cause = e.cause
            raise ApplicationError(
                str(cause) if cause else "subagent rejected the message",
                type=getattr(cause, "type", None) or "SubagentSendRejected",
                non_retryable=True,
            ) from e
        return _TurnProgress(
            sent=True,
            turn_id=result.turn_id,
            turn_number=result.turn_number,
            message_id=result.message_id,
        )

    @staticmethod
    async def _publish_dispatch(
        req: RunSubagentTurnInput, progress: _TurnProgress
    ) -> None:
        """Publish the :class:`SubagentMessageSent` marker onto the PARENT's stream.

        Called once per fresh send on Workflow Streams (see the caller's dedup note). Uses
        ``AgentWorkflowRunner.publisher_from_activity``, which targets THIS activity's own parent
        workflow (the agent that dispatched the activity) — so the event lands on the parent's
        stream, never the child's (stream isolation). ``subagent_turn`` is the child's actual
        accepted turn number from the send (``progress.turn_number``)."""
        async with AgentWorkflowRunner.publisher_from_activity(
            req.parent_stream_context
        ) as publisher:
            publisher.publish(
                SubagentMessageSent(
                    subagent_id=req.handle,
                    agent_key=req.agent_key,
                    workflow_id=req.child_workflow_id,
                    handler=req.type,
                    subagent_turn=progress.turn_number,
                    # The child stream offset this turn's events begin at (the perf-hint offset
                    # the parent resumed from) — lets a client merging the parent + child streams
                    # mount the child cursor at the right spot when resuming mid-session.
                    from_offset=req.from_offset,
                )
            )

    @staticmethod
    def _resume_progress() -> _TurnProgress | None:
        """The most recent heartbeat memo for this activity attempt, or ``None`` if the
        message has not been sent yet. The pydantic converter decodes ``heartbeat_details``
        to plain values, so we re-validate the latest into :class:`_TurnProgress`."""
        details = activity.info().heartbeat_details
        if not details:
            return None
        return _TurnProgress.model_validate(details[-1])
