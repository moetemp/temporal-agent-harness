"""Which event-stream backend the test agents use, and a reader that works on both.

With ``STREAMS_STORE=memory`` or ``redis`` the test agents pass no ``stream=``, so they use the
default ``ContribStream`` on that store, and ``tests/conftest.py`` puts the Stream plugin on
every test client. ``redis`` needs a Redis at ``STREAMS_REDIS_URL``. The lane puts its keys
under a prefix of its own, ``STREAMS_KEY_PREFIX`` unless it is set, so two runs on one Redis
never meet. Without ``STREAMS_STORE`` they pass ``stream=WorkflowStream()``, which a released
SDK has. The Bazel build runs them so.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStream, WorkflowStreamClient

from temporal_agent_harness.harness.agent_protocol import (
    AGENT_STATUS_QUERY,
    TURN_EVENTS_TOPIC,
    AgentEvent,
    AgentStatus,
)

# Read once at import, outside any workflow, so the workflow code that calls ``agent_stream``
# reads no environment.
STORE = os.environ.get("STREAMS_STORE") or None
if STORE:
    os.environ.setdefault("STREAMS_KEY_PREFIX", f"harness-test-{uuid.uuid4().hex[:12]}")


def stream_plugins() -> list[Any]:
    """The lane's Stream plugin for a client or ``Replayer`` that the conftest does not build."""
    if STORE is None:
        return []
    from temporal_agent_harness.harness.contrib_stream import streams_from_env

    return [streams_from_env()]


def agent_stream() -> WorkflowStream | None:
    """The ``stream=`` choice of a test agent, which calls it from its ``__init__``.

    ``None`` on a Stream lane gives the runner's default ``ContribStream``.
    """
    if STORE is not None:
        return None
    return _OwnWorkflowStream().stream


class _OwnWorkflowStream:
    def __init__(self) -> None:
        # ``WorkflowStream`` accepts only a caller named ``__init__``. The test agent's own
        # ``__init__`` calls ``agent_stream``, so the stream is still built at workflow init.
        self.stream = WorkflowStream()


async def turn_events(
    client: Client, workflow_id: str, *, after: str = ""
) -> AsyncIterator[AgentEvent]:
    """Yield ``workflow_id``'s own turn events from the beginning (or a cursor), tailing live."""
    status = await client.get_workflow_handle(workflow_id).query(
        AGENT_STATUS_QUERY, result_type=AgentStatus
    )
    if status.stream is None:
        stream = WorkflowStreamClient.create(client, workflow_id)
        async for item in stream.subscribe(
            topics=[TURN_EVENTS_TOPIC], from_offset=int(after or 0), result_type=AgentEvent
        ):
            yield item.data
        return
    from temporal_agent_harness.harness.contrib_stream import read_turn_events

    items = read_turn_events(client, status.stream, workflow_id=workflow_id, after=after)
    try:
        async for item in items:
            yield item.event
    finally:
        await items.aclose()


class TurnRecord:
    """An event with the ``.data`` shape of a Workflow Streams item, for tests written for it."""

    def __init__(self, data: AgentEvent) -> None:
        self.data = data


async def turn_records(
    client: Client, workflow_id: str, *, after: str = ""
) -> AsyncIterator[TurnRecord]:
    """``turn_events``, each wrapped as a ``TurnRecord``."""
    async for event in turn_events(client, workflow_id, after=after):
        yield TurnRecord(event)
