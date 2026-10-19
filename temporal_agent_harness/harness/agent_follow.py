# ABOUTME: The client side of the unified subagent event stream. One ``follow()`` of the root
# agent's turn events reads the root and, through the Link Records the agents write, every
# subagent stream they drive, in one order. This module turns the follow's items into the
# harness's events and its resume point.
#
# The order rules live in the stream runtime, not here. A child's records come after the OPEN
# that its parent writes with ``subagent_message_sent``. A parent's records after a JOIN (written
# just before ``subagent_reply_received``) wait until the child reached its ``turn_end``. Records
# that no bracket relates come in the order the follow read them, and no clock decides it.

from __future__ import annotations

import base64
import binascii
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass, replace
from typing import Any

from temporalio.client import Client
from temporalio.contrib.streams import (
    Cursor,
    FollowedItem,
    FollowPosition,
    LinkEvent,
    LinkStateKind,
    RecordKind,
    StreamCursorError,
    StreamError,
    stream_handle,
)

from temporal_agent_harness.harness.agent_protocol import (
    TURN_EVENTS,
    AgentEvent,
    AgentEventType,
    SubagentStreamUnavailable,
)
from temporal_agent_harness.harness.stream_transport import cursor, turn_event

_log = logging.getLogger(__name__)

# The follow's own defaults. A subagent tree deeper or wider than this shows the parent with a
# ``subagent_stream_unavailable`` marker in place of the subagent's events.
DEFAULT_SUBAGENT_MAX_DEPTH = 4
DEFAULT_SUBAGENT_MAX_OPEN = 16

# A link that the follow skips because the same stream is open already loses nothing, since
# the open segment reads on through the stream. So it gets no marker.
_SKIP_WITH_NO_LOSS = "open"


@dataclass(frozen=True)
class ResumePoint:
    """Where a consumer of the unified stream left off.

    ``cursor`` is the root stream's cursor and ``state`` the follow state, which holds the cursor
    of each open subagent stream. With both, a resume inside a subagent turn gets the rest of
    that turn. ``seq`` is the root agent's own count of the events it published
    (``AgentEvent.seq``), which tells a consumer that it has caught up with the agent
    (``AgentStatus.last_event_seq``). The point encodes to one string for a UI or a Nexus caller
    to store. A point with no state (the older ``seq@cursor`` form) resumes the root only.
    """

    cursor: str = ""
    seq: int = 0
    state: str = ""

    def encode(self) -> str:
        """One string for a consumer to store and hand back.

        The cursor token names the store that minted it, so a point handed to another store is
        refused when the follow opens.
        """
        if not (self.cursor or self.seq or self.state):
            return ""
        text = f"{self.seq}@{self.cursor}"
        return f"{text}#{self.state}" if self.state else text

    @classmethod
    def decode(cls, text: str) -> ResumePoint:
        """The point ``text`` encodes. Raises ``ValueError`` when the text is malformed."""
        if not text:
            return cls()
        seq, sep, rest = text.partition("@")
        if not sep:
            raise ValueError(f"not a resume point: {text!r}")
        # A cursor token has no '#', and base64 has none either.
        token, _, state = rest.partition("#")
        try:
            point = cls(cursor=token, seq=int(seq), state=state)
            base64.b64decode(state, validate=True)
        except (ValueError, binascii.Error):
            raise ValueError(f"not a resume point: {text!r}") from None
        return point

    def position(self) -> Cursor | FollowPosition:
        """What ``follow()`` resumes after."""
        if self.state:
            return FollowPosition(self.cursor, self.state)
        return cursor(self.cursor)


@dataclass(frozen=True)
class FollowedEvent:
    """One event of the unified stream."""

    event: AgentEvent
    root: bool
    """The event is the root agent's own, not a subagent's."""
    resume: ResumePoint
    """Where to resume so that nothing after this event is lost.

    The follow gives a position at the end of each batch only. So the last event of a batch
    carries the point after the batch, and the others carry the point before it, which repeats
    the rest of the batch on a resume."""


async def follow_agent_events(
    client: Client,
    workflow_id: str,
    *,
    resume: ResumePoint,
    skip_until_turn_id: str | None = None,
    max_depth: int = DEFAULT_SUBAGENT_MAX_DEPTH,
    max_open: int = DEFAULT_SUBAGENT_MAX_OPEN,
) -> AsyncGenerator[FollowedEvent, None]:
    """Yield ``workflow_id``'s turn events and those of its subagents after ``resume``.

    With ``skip_until_turn_id``, the root's events before that turn's ``turn_started`` are
    dropped, and so are the events of each subagent stream that a dropped record opened. Each
    subagent stream that can't be read, or that the follow skips past ``max_depth`` or
    ``max_open``, gives one ``subagent_stream_unavailable`` event with the reason. The parent's
    events go on. A root that can't be read ends the stream, and a consumer recovers with a new
    follow. A cursor the store refuses raises ``StreamCursorError``. Close the generator to stop
    the follow. ``client`` carries the stream store as a plugin.
    """
    handle = stream_handle(client, workflow_id)
    items = handle.follow(
        topic=TURN_EVENTS,
        after=resume.position(),
        max_depth=max_depth,
        max_open=max_open,
    )
    stream = _UnifiedStream(skip_until_turn_id)
    point = resume
    batch: list[tuple[AgentEvent, bool]] = []
    try:
        async for item in items:
            event = stream.event(item)
            if event is not None:
                batch.append((event, item.depth == 0))
            if item.position is None:
                continue
            after = point
            for event, root in batch:
                if root and event.seq is not None:
                    after = replace(after, seq=event.seq)
            after = replace(after, cursor=item.position.cursor, state=item.position.state)
            for index, (event, root) in enumerate(batch):
                yield FollowedEvent(event, root, after if index == len(batch) - 1 else point)
            point, batch = after, []
        for event, root in batch:
            yield FollowedEvent(event, root, point)
    except StreamCursorError:
        raise
    except StreamError as error:
        # An exception out of the generator would fail an SSE response in the middle.
        _log.warning("follow of %s ended after a read error: %r", workflow_id, error)
    finally:
        await items.aclose()


def _key(workflow_id: str, topic: str) -> tuple[str, str]:
    return workflow_id, topic


class _UnifiedStream:
    """The per-source state that turns follow items into events."""

    def __init__(self, skip_until_turn_id: str | None) -> None:
        self._skip_until_turn_id = skip_until_turn_id
        # ``None`` takes every source. Else only the sources that an included record opened, and
        # the root once it reached its turn.
        self._included: set[tuple[str, str]] | None = (
            None if skip_until_turn_id is None else set()
        )
        # Each source's last event, which a supersession on that source is stamped with.
        self._previous: dict[tuple[str, str], AgentEvent] = {}

    def event(self, item: FollowedItem[Any]) -> AgentEvent | None:
        source = _key(item.source.workflow_id, item.source.topic)
        if item.link_state is not None:
            if not self._takes(source):
                return None
            return _unavailable(item)
        record = item.record
        if record is None:
            return None
        if record.kind is RecordKind.LINK:
            link = record.link
            if (
                link is not None
                and link.event is LinkEvent.OPEN
                and link.ref is not None
                and self._included is not None
                and source in self._included
            ):
                self._included.add(_key(link.ref.workflow_id, link.ref.topic))
            return None
        event = turn_event(record, self._previous.get(source))
        if event is None:
            return None
        if not isinstance(event, AgentEvent):
            # The follow types the root's records only, since a child's topic may hold another
            # type. Here every source is a turn-events topic.
            event = AgentEvent.model_validate(event)
        if record.kind is RecordKind.DATA:
            self._previous[source] = event
        if (
            self._included is not None
            and item.depth == 0
            and source not in self._included
            and event.event.type == AgentEventType.TURN_STARTED
            and event.turn_id == self._skip_until_turn_id
        ):
            self._included.add(source)
        return event if self._takes(source) else None

    def _takes(self, source: tuple[str, str]) -> bool:
        return self._included is None or source in self._included


def _unavailable(item: FollowedItem[Any]) -> AgentEvent | None:
    """The ``subagent_stream_unavailable`` event for a link report, or ``None`` for none."""
    state = item.link_state
    assert state is not None
    if state.kind is LinkStateKind.UNAVAILABLE:
        reason = f"subagent stream unavailable: {state.error}"
    elif state.kind is LinkStateKind.SKIPPED and state.reason != _SKIP_WITH_NO_LOSS:
        reason = f"subagent stream not followed: {state.reason}"
    else:
        return None
    link = state.link
    subagent_id = link.name.rpartition("#")[0] or link.name
    return AgentEvent(
        agent_id=subagent_id,
        turn_id="",
        turn_number=0,
        timestamp=0.0,
        event=SubagentStreamUnavailable(
            subagent_id=subagent_id,
            workflow_id=link.ref.workflow_id if link.ref is not None else "",
            reason=reason,
        ),
    )
