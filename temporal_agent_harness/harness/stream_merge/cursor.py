# ABOUTME: One mounted stream's cursor for the merge, a thin peek-ahead wrapper over a single
# agent workflow's turn-event subscription. Holds at most one peeked ``head`` event, runs at most
# one in-flight ``pull`` task, and applies the root-only "skip to my turn_started" preamble that
# establishes the quiescent start point. The engine (merge.py) owns scheduling across cursors;
# this type owns only one stream's read position.

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

from temporalio.client import Client
from temporalio.streams import StreamRecord

from temporal_agent_harness.harness.agent_protocol import AgentEvent, AgentEventType
from temporal_agent_harness.harness.stream_transport import follow_turn_events

_log = logging.getLogger(__name__)


class Cursor:
    """A peek-ahead read position over one agent workflow's event stream.

    The engine keeps one ``Cursor`` per mounted agent (the root, plus each subagent mounted when
    its ``subagent_message_sent`` is emitted). Invariant: a cursor has EITHER a buffered
    :attr:`head` (the next event, awaiting emission) OR an in-flight :attr:`pull_task` fetching one
    OR is :attr:`exhausted`, never a head and a pull at once.
    """

    def __init__(
        self,
        *,
        workflow_id: str,
        is_child: bool,
        mount_index: int,
        events: AsyncIterator[StreamRecord[AgentEvent]],
        skip_until_turn_id: str | None = None,
    ) -> None:
        self.workflow_id = workflow_id
        # False only for the root cursor; the open gate applies to child cursors only.
        self.is_child = is_child
        # Order this stream was mounted in (root = 0). The replay select policy ranks by it for a
        # deterministic, clock-free interleaving.
        self.mount_index = mount_index
        self._events = events
        # Root-only preamble (send_message): discard events until a SPECIFIC turn's ``turn_started``
        # (the submitted turn), which becomes the first head. ``None`` means no skipping, used by
        # BOTH ``attach`` from the beginning (replay everything) AND ``attach`` resuming after a
        # stored cursor (start exactly there; a subagent whose turn began earlier is simply never
        # mounted, see the merge's unmounted-stuck give-up). Children never skip.
        self._skip_until_turn_id = skip_until_turn_id
        self._skipping = skip_until_turn_id is not None

        self.head: AgentEvent | None = None
        # The provider's cursor for the buffered head, on THIS stream. The engine records it as it
        # emits each ROOT event, as the position a consumer resumes after (subagent heads never
        # move that position). Opaque: it is stored and handed back, never compared. Empty until a
        # head is buffered.
        self.head_cursor: str = ""
        # Arrival order among completed pulls, the live select policy's tiebreak (set by engine).
        self.head_seq: int = -1
        self.exhausted = False
        self.pull_task: asyncio.Task[None] | None = None
        # Set when a pull raised something other than normal end-of-stream (e.g. the per-workflow
        # concurrent-update cap, or a transient RPC error). It marks the cursor unreadable so the
        # engine can DEGRADE GRACEFULLY: drop a failed CHILD cursor and keep coalescing the rest,
        # rather than letting one unreadable subagent crash the whole merged stream.
        self.error: BaseException | None = None

    @classmethod
    def mount(
        cls,
        client: Client,
        *,
        workflow_id: str,
        is_child: bool,
        mount_index: int,
        after: str,
        skip_until_turn_id: str | None = None,
    ) -> "Cursor":
        """Subscribe to ``workflow_id``'s turn events after ``after`` and wrap it as a cursor.

        ``after`` is a stored cursor token (empty for the beginning). The subscription opens on
        the first pull, so mounting itself does no I/O.
        """
        return cls(
            workflow_id=workflow_id,
            is_child=is_child,
            mount_index=mount_index,
            events=follow_turn_events(client, workflow_id, after=after),
            skip_until_turn_id=skip_until_turn_id,
        )

    async def pull(self) -> None:
        """Advance the stream until :attr:`head` holds the next emittable event (or exhaust it).

        Honors the skip preamble: while skipping, events are discarded WITHOUT being emitted or
        recorded (so any prior-turn subagent brackets in that tail never enter the merge) until
        this turn's ``turn_started``, which IS kept as the first head. The subscription live-tails,
        so on an idle agent this simply awaits the next event; it raises ``StopAsyncIteration``
        only when the workflow has terminated, which sets :attr:`exhausted`.

        Any OTHER error (notably the per-workflow concurrent-in-flight-update cap, raised as an
        ``RPCError`` when too many cursors poll one workflow at once, or a poll against an
        already-completed child) is captured on :attr:`error` and marks the cursor exhausted rather
        than propagating, so the engine can drop this cursor and keep coalescing the rest. A
        ``CancelledError`` (teardown) is re-raised untouched.
        """
        try:
            while True:
                record = await anext(self._events)
                ev = record.value
                if self._skipping:
                    if (
                        ev.event.type == AgentEventType.TURN_STARTED
                        and ev.turn_id == self._skip_until_turn_id
                    ):
                        self._skipping = False  # keep this turn_started as the first head
                    else:
                        continue  # discard the prior turn's tail
                self.head = ev
                self.head_cursor = record.cursor.token
                return
        except StopAsyncIteration:
            self.exhausted = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- see docstring: record, don't crash the merge
            self.error = exc
            self.exhausted = True
            _log.warning(
                "stream_merge: dropping cursor for workflow %s after a read error: %r",
                self.workflow_id,
                exc,
            )

    async def aclose(self) -> None:
        """Cancel any in-flight pull and close the underlying subscription (idempotent)."""
        if self.pull_task is not None and not self.pull_task.done():
            self.pull_task.cancel()
            try:
                await self.pull_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001 -- teardown best-effort; never mask the real exit
                pass
        aclose = getattr(self._events, "aclose", None)
        if aclose is not None:
            try:
                await aclose()
            except Exception:  # noqa: BLE001 -- best-effort
                pass
