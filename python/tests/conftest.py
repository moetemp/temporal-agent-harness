# ABOUTME: Points WorkflowEnvironment at pre-installed Temporal servers when the environment names them.
#
# TEMPORAL_CLI_PATH backs start_local (the `temporal` dev server) and TEMPORAL_TEST_SERVER_PATH
# backs start_time_skipping (the Java test server). Bazel sets both to the binaries pinned in
# MODULE.bazel, so a sandboxed test never downloads a server. With neither set, as under a plain
# `uv run pytest`, the SDK downloads them as it always has. An explicit path argument at a call
# site still wins.
#
# MONTY_BIN, read by pydantic-monty, is made absolute for the same reason the server paths are:
# BUILD files name it relative to the runfiles root, and tests change directory.

from __future__ import annotations

import functools
import os
from collections.abc import Awaitable, Callable
from typing import Any

from temporalio.testing import WorkflowEnvironment


def _default_server_path(name: str, parameter: str, env_var: str) -> None:
    path = os.environ.get(env_var)
    if not path:
        return
    path = os.path.abspath(path)
    start: Callable[..., Awaitable[WorkflowEnvironment]] = getattr(WorkflowEnvironment, name).__func__

    @functools.wraps(start)
    async def start_with_path(cls: type[WorkflowEnvironment], *args: Any, **kwargs: Any) -> WorkflowEnvironment:
        kwargs.setdefault(parameter, path)
        return await start(cls, *args, **kwargs)

    setattr(WorkflowEnvironment, name, classmethod(start_with_path))


if os.environ.get("MONTY_BIN"):
    os.environ["MONTY_BIN"] = os.path.abspath(os.environ["MONTY_BIN"])

_default_server_path("start_local", "dev_server_existing_path", "TEMPORAL_CLI_PATH")
_default_server_path("start_time_skipping", "test_server_existing_path", "TEMPORAL_TEST_SERVER_PATH")


def _streams_lane() -> None:
    """With ``STREAMS_STORE`` set, give every test client the Stream Store it names.

    ``WorkflowEnvironment.start_*`` take no Store, so the environment's client is built again
    from its own config with the Store added. ``TEMPORAL_ADDRESS`` then gives a running server
    that ``start_time_skipping`` connects to, since the Redis lane runs on a dev server.
    ``tests/_streams.py`` has the rest of the lane.

    The client also gets a plugin with the Local Activities that write the Stream for Workflow
    code. ``AgentHarnessPlugin`` registers them, and many test Workers run their agents without
    it.
    """
    if not os.environ.get("STREAMS_STORE"):
        return
    from temporalio.client import Client
    from temporalio.plugin import SimplePlugin

    from temporal_agent_harness.harness.contrib_stream import stream_stores_from_env
    from temporal_agent_harness.harness.contrib_stream_activities import (
        append_events,
        create_event_stream,
    )
    from temporal_agent_harness.plugin import _merge_activities

    class _StreamActivities(SimplePlugin):
        def __init__(self) -> None:
            super().__init__(
                "StreamLaneActivities",
                activities=lambda found: _merge_activities(
                    found, [create_event_stream, append_events]
                ),
            )

    def with_lane_plugin(plugins: Any) -> list[Any]:
        return [*(plugins or []), _StreamActivities()]

    # Read once, since a test of the web app's CLI sets TEMPORAL_ADDRESS for its own process.
    address = os.environ.get("TEMPORAL_ADDRESS")
    for name in ("start_local", "start_time_skipping"):
        start = getattr(WorkflowEnvironment, name).__func__

        def wrap(start: Any, name: str) -> Any:
            @functools.wraps(start)
            async def start_with_streams(
                cls: type[WorkflowEnvironment], *args: Any, **kwargs: Any
            ) -> WorkflowEnvironment:
                stores = stream_stores_from_env()
                if name == "start_time_skipping" and address:
                    kwargs.pop("test_server_existing_path", None)
                    plugins = with_lane_plugin(kwargs.pop("plugins", None))
                    client = await Client.connect(
                        address, **kwargs, plugins=plugins, stream_stores=stores
                    )
                    return WorkflowEnvironment.from_client(client)
                env = await start(cls, *args, **kwargs)
                config = env.client.config()
                return _WithStore(
                    env,
                    Client(
                        **{
                            **config,
                            "plugins": with_lane_plugin(config["plugins"]),
                            "stream_stores": stores,
                        }
                    ),
                )

            return classmethod(start_with_streams)

        setattr(WorkflowEnvironment, name, wrap(start, name))


class _WithStore(WorkflowEnvironment):
    """An environment whose client has the lane's Store. The rest is the started environment's,
    which owns the server and its time skipping."""

    def __init__(self, env: WorkflowEnvironment, client: Any) -> None:
        super().__init__(client)
        self._env = env

    async def shutdown(self) -> None:
        await self._env.shutdown()

    async def sleep(self, duration: Any) -> None:
        await self._env.sleep(duration)

    async def get_current_time(self) -> Any:
        return await self._env.get_current_time()

    @property
    def supports_time_skipping(self) -> bool:
        return self._env.supports_time_skipping

    @property
    def supports_time_skipping_v1(self) -> bool:
        return self._env.supports_time_skipping_v1

    @property
    def supports_time_skipping_v2(self) -> bool:
        return self._env.supports_time_skipping_v2

    def auto_time_skipping_disabled(self) -> Any:
        return self._env.auto_time_skipping_disabled()


_streams_lane()
