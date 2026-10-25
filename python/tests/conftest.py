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
    """With ``STREAMS_STORE`` set, give every test client the Stream backend it names.

    ``TEMPORAL_ADDRESS`` then gives a running server that ``start_time_skipping`` connects to,
    since the Redis lane runs on a dev server. ``tests/_streams.py`` has the rest of the lane.
    """
    if not os.environ.get("STREAMS_STORE"):
        return
    from temporalio.client import Client

    from temporal_agent_harness.harness.contrib_stream import stream_backends_from_env

    # Read once, since a test of the web app's CLI sets TEMPORAL_ADDRESS for its own process.
    address = os.environ.get("TEMPORAL_ADDRESS")
    for name in ("start_local", "start_time_skipping"):
        start = getattr(WorkflowEnvironment, name).__func__

        def wrap(start: Any, name: str) -> Any:
            @functools.wraps(start)
            async def start_with_streams(
                cls: type[WorkflowEnvironment], *args: Any, **kwargs: Any
            ) -> WorkflowEnvironment:
                kwargs["stream_backends"] = stream_backends_from_env()
                if name == "start_time_skipping" and address:
                    kwargs.pop("test_server_existing_path", None)
                    client = await Client.connect(address, **kwargs)
                    return WorkflowEnvironment.from_client(client)
                return await start(cls, *args, **kwargs)

            return classmethod(start_with_streams)

        setattr(WorkflowEnvironment, name, wrap(start, name))


_streams_lane()
