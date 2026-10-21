"""Where the stream-backed tests get their Stream plugin, their server, and their turn events.

The Streams live in a Redis at ``STREAMS_REDIS_URL`` (``redis://127.0.0.1:6379`` by default),
so the suite needs one. The suite puts its keys under a prefix of its own, ``STREAMS_KEY_PREFIX``
unless it is set, so two runs on one Redis never meet. ``TEMPORAL_ADDRESS`` names a running server
for the tests to connect to. Without it they start the time-skipping test server.

The plugin is registered on the test client, through ``Client.connect``, the way an application
registers it. Workers built from that client take it, and every reader goes through
``for_handle``. One plugin serves one client connection, so every test builds its own.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

from temporalio.client import Client
from temporalio.contrib.streams.redis import RedisStreamPlugin
from temporalio.testing import WorkflowEnvironment

from temporal_agent_harness.harness.agent_protocol import AgentEvent
from temporal_agent_harness.harness.stream_transport import (
    KEY_PREFIX_ENV,
    follow_turn_events,
    streams_from_env,
)

__all__ = [
    "reset_store",
    "store",
    "streaming_client",
    "turn_events",
    "workflow_environment",
]

# Every plugin of this process reads the prefix from the environment, the web app's too.
os.environ.setdefault(KEY_PREFIX_ENV, f"harness-test-{uuid.uuid4().hex[:12]}")

_current: RedisStreamPlugin | None = None


def store() -> RedisStreamPlugin:
    """The plugin of the running test, built on first use and dropped by the suite's fixture."""
    global _current
    if _current is None:
        _current = streams_from_env()
    return _current


def reset_store() -> None:
    """Drop the running test's plugin, so the next test builds one on its own client."""
    global _current
    _current = None


async def streaming_client(client: Client) -> Client:
    """A new client on ``client``'s server with a fresh plugin, for a shared environment.

    A Worker takes the plugin only from a client that ``Client.connect`` made with it, so this
    connects again and does not copy ``client``.
    """
    config = client.config()
    return await Client.connect(
        client.service_client.config.target_host,
        namespace=client.namespace,
        data_converter=config["data_converter"],
        plugins=[*config["plugins"], streams_from_env()],
    )


async def workflow_environment(**kwargs: Any) -> WorkflowEnvironment:
    """A server for the test, with the test's plugin on the client.

    ``TEMPORAL_ADDRESS`` names a running server. Without it, the time-skipping test server
    starts.
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
    """Yield ``workflow_id``'s turn events from the beginning (or a cursor), tailing live."""
    records = follow_turn_events(client, workflow_id, after=after)
    try:
        async for record in records:
            yield record.data
    finally:
        await records.aclose()
