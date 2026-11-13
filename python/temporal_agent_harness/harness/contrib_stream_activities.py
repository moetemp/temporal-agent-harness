# ABOUTME: The two Local Activities of the harness that write a ``ContribStream`` Stream for
# Workflow code. Workflow code can't do I/O, so ``ContribSink`` calls them, and each one uses the
# client's Streams API through ``activity.client()``, as any Activity does.
#
# ``AgentHarnessPlugin`` registers both on every Worker. This module imports the Streams API only
# inside the Activities, so a Worker on an SDK without that API can still register them.

from __future__ import annotations

from typing import Any

from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from temporal_agent_harness.harness.agent_protocol import AgentEvent

CREATE_EVENT_STREAM_ACTIVITY = "agent_harness.create_event_stream"
APPEND_EVENTS_ACTIVITY = "agent_harness.append_events"

NO_STORE = (
    "stream=ContribStream() needs a Stream Store on the client, such as "
    "Client.connect(..., plugins=[AgentHarnessPlugin()], stream_stores=stream_stores_from_env()) "
    "with STREAMS_STORE set. stream=WorkflowStream() needs none."
)


def has_store(client: Client) -> bool:
    """Whether ``client`` has a Stream Store. A client takes at most one."""
    return bool(client.config(active_config=True)["stream_stores"])


def _client() -> Client:
    client = activity.client()
    if not has_store(client):
        # Without a Store the call goes to the server, which fails it with a message that
        # names no setup. A retry can't add the Store, so the run fails at once.
        raise ApplicationError(NO_STORE, type="NoStreamStore", non_retryable=True)
    return client


@activity.defn(name=CREATE_EVENT_STREAM_ACTIVITY)
async def create_event_stream(stream_id: str) -> dict[str, Any]:
    """Create the root agent's Stream ``stream_id``, or find it, and give its ref as data.

    Each attempt sends a new request id, so a strict create would fail on a later run of the
    chain. The id holds the workflow id and the first run id, so a Stream that exists is the
    chain's own.
    """
    from temporalio.nexus.system.stream_service import StreamIdConflictPolicy

    from temporal_agent_harness.harness._contrib_stream_parts import ref_to_data

    stream = await _client().create_stream(
        id=stream_id,
        body_type=AgentEvent,
        id_conflict_policy=StreamIdConflictPolicy.StreamIdConflictPolicyUseExisting,
    )
    return ref_to_data(stream.ref)


@activity.defn(name=APPEND_EVENTS_ACTIVITY)
async def append_events(ref: dict[str, Any], events: list[AgentEvent]) -> str:
    """Append ``events`` to the Stream at ``ref`` in one call, and give the last offset in hex.

    The SDK writes as this Local Activity's Producer. A retried Workflow Task runs the same call
    with a higher epoch, so a reader withdraws the Records of the failed Task. A ``StreamError``
    of any reason but a Store outage is non-retryable, so only an outage retries.
    """
    from temporal_agent_harness.harness._contrib_stream_parts import ref_from_data

    stream = _client().get_stream(ref_from_data(ref), body_type=AgentEvent)
    written = await stream.append(*events)
    return written.last_offset.hex()
