"""Where the stream-backed tests get their store, their server, and their turn events.

``STREAMS_STORE`` picks the store the way it does for a worker, but the suite defaults to
``memory``, so it needs nothing installed. ``redis`` needs a Redis at ``STREAMS_REDIS_URL``.
``TEMPORAL_ADDRESS`` names a running server for the tests to connect to. Without it they start
the time-skipping test server. ``STREAMS_NOTIFY=1`` needs a server with the stream notifier.

The store is registered on the test client as a plugin, through ``Client.connect``, the way an
application registers it. Workers built from that client take it, and every reader goes
through ``get_stream_handle(client, ...)``. One store serves one client connection, so every
test builds its own.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

from temporalio.client import Client
from temporalio.contrib.streams import StreamStorePlugin
from temporalio.testing import WorkflowEnvironment

from temporal_agent_harness.harness.agent_protocol import AgentEvent
from temporal_agent_harness.harness.stream_transport import follow_turn_events, store_from_env

__all__ = [
    "reset_store",
    "store",
    "streaming_client",
    "turn_events",
    "workflow_environment",
]

TEST_DEFAULT_STORE = "memory"

_current: StreamStorePlugin | None = None


def store() -> StreamStorePlugin:
    """The store of the running test, built on first use and dropped by the suite's fixture."""
    global _current
    if _current is None:
        _current = store_from_env(TEST_DEFAULT_STORE)
    return _current


def reset_store() -> None:
    """Drop the running test's store, so the next test builds one on its own client."""
    global _current
    _current = None


async def streaming_client(client: Client) -> Client:
    """A new client on ``client``'s server with a fresh store, for a shared environment.

    A Worker takes a store only from a client that ``Client.connect`` made with it, so this
    connects again and does not copy ``client``.
    """
    config = client.config()
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        data_converter=config["data_converter"],
        plugins=[*config["plugins"], store_from_env(TEST_DEFAULT_STORE)],
    )


async def workflow_environment(**kwargs: Any) -> WorkflowEnvironment:
    """A server for the test, with the test's store on the client.

    ``TEMPORAL_ADDRESS`` names a running server, such as one with the stream notifier. Without
    it, the time-skipping test server starts.
    """
    plugins = [*kwargs.pop("plugins", []), store()]
    address = os.environ.get("TEMPORAL_ADDRESS")
    if address:
        client = await Client.connect(address, plugins=plugins, **kwargs)
        return WorkflowEnvironment.from_client(client)
    return await WorkflowEnvironment.start_time_skipping(plugins=plugins, **kwargs)


async def turn_events(
    client: Client, workflow_id: str, *, after: str = ""
) -> AsyncIterator[AgentEvent]:
    """Yield ``workflow_id``'s turn events from the beginning (or after a cursor), tailing live."""
    records = follow_turn_events(client, workflow_id, after=after)
    try:
        async for record in records:
            yield record.value
    finally:
        await records.aclose()
