"""Where the stream-backed tests get their provider, their server, and their turn events.

``STREAMS_PROVIDER`` picks the provider the way it does for a worker. The default,
``workflow_streams``, is today's transport and runs on the time-skipping test server, so the
suite needs nothing installed. ``native`` needs a server built from the stream branch:
``TEMPORAL_ADDRESS`` names it and the tests connect there instead. ``redis`` needs a Redis at
``AI198_REDIS_URL`` and runs on the test server like the default. ``memory`` is the in-process
reference provider.

The provider is registered on the test client as a plugin, the way an application registers
it, so workers built from that client inherit it and every reader goes through
``client.get_stream_handle()``. One provider serves one test: a provider that holds connections
binds them to the event loop it first used, and every test has its own loop.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

from temporalio.client import Client
from temporalio.streams import StreamProvider
from temporalio.testing import WorkflowEnvironment

from temporal_agent_harness.harness.agent_protocol import AgentEvent
from temporal_agent_harness.harness.stream_transport import (
    follow_turn_events,
    provider_from_env,
    provider_name,
)

__all__ = [
    "close_provider",
    "provider",
    "streaming_client",
    "turn_events",
    "workflow_environment",
]

_current: StreamProvider | None = None


def provider() -> StreamProvider:
    """The provider of the running test, built on first use and closed by the suite's fixture."""
    global _current
    if _current is None:
        _current = provider_from_env()
    return _current


async def close_provider() -> None:
    """Close the running test's provider, if it built one, so the next test starts fresh."""
    global _current
    current, _current = _current, None
    if current is not None:
        await current.close()


def streaming_client(client: Client) -> Client:
    """``client`` with this test's provider registered, for an environment shared by tests."""
    config = client.config()
    config["plugins"] = [*config["plugins"], provider()]
    return Client(**config)


async def workflow_environment(**kwargs: Any) -> WorkflowEnvironment:
    """A server the configured provider can run on, with the provider on the client.

    The native provider's streams live in the server, and no released server carries them, so
    that provider connects to the one ``TEMPORAL_ADDRESS`` names rather than starting the
    time-skipping one.
    """
    plugins = [*kwargs.pop("plugins", []), provider()]
    if provider_name() == "native":
        address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
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
