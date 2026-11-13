# Temporal Agent Harness for Python

Build durable agents using Python, Temporal, and your preferred AI SDK.

[Project overview](https://github.com/temporal-community/temporal-agent-harness/blob/main/README.md) · [TypeScript guide](https://github.com/temporal-community/temporal-agent-harness/blob/main/typescript/packages/core/README.md) · [Harness API](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/api/README.md)

<a id="coding-agent-setup-and-starter"></a>

## Quickstart

Create an agent, install its coding-agent skill, and open a working session in four steps.
No repository checkout or Bazel installation is needed.

> **Available with the next Python release.** The `init`, `skills`, `dev`, and `doctor` commands
> are not in the published 0.8.0 package. Once the new release is published, the `@latest`
> command below selects it. Until then, use the [manual setup](#build-with-it--install-from-pypi)
> with published packages.

You need **Python 3.11+**, [uv](https://docs.astral.sh/uv/getting-started/installation/), the
[Temporal CLI](https://docs.temporal.io/cli), and an OpenAI API key for the starter's `ask` handler.

### 1. Create the project

Run this from the directory where you want the new project:

```bash
uvx temporal-agent-harness@latest init my-agent
cd my-agent
uv sync
```

`uvx` supplies the CLI before you have a project. `init` creates `my_agent/workflow.py`,
`my_agent/worker.py`, `agents.toml`, a `pyproject.toml` named after the directory, and a README.
It pins the installed harness version with the `ui` and `openai-agents` extras and refuses to
overwrite existing files. `uv sync` installs the project and its dependencies. The starter's
weather tool returns sample data.

### 2. Configure it and install the skill

```bash
cp env.example .env.local
uv run temporal-agent-harness schema my_agent.workflow:OpenAIHelloAgentWorkflow -o agent.schema.json
uv run temporal-agent-harness skills install   # asks which coding agents to install the skill for
```

`.env.local` is where `OPENAI_API_KEY` goes once you try `ask`; never commit it. `schema` exports
your agent's handlers and types to `agent.schema.json`. `skills install` asks which coding agents
you use and copies this SDK release's skill, with authoring, API, advanced-feature, and
verification references, into the directories they read:

| Choice | `--for` | Skill directory | Read by |
| --- | --- | --- | --- |
| 1 | `claude` | `.claude/skills` | Claude Code |
| 2 | `agents` | `.agents/skills` | Codex and other agents that read the shared `.agents/skills` directory |

Choose both if you use Claude Code alongside another agent. To skip the question, as in scripts,
pass the choices: `skills install --for claude --for agents`.
Installing the Python package bundles the
skill; this explicit step makes it discoverable in your project. For a tool with another skill
directory, run `uv run temporal-agent-harness skills path` to locate the bundled resources, or
use `skills install --dest <directory>`.

### 3. Start the three services

Run each command in its own terminal, all from this project directory:

**Terminal 1 — Temporal service** (skip if your configured Temporal service is already running):

```bash
temporal server start-dev
```

**Terminal 2 — shared session manager, API, and harness console:**

```bash
uv run temporal-agent-harness dev agents.toml
```

**Terminal 3 — your Python agent worker:**

```bash
uv run --env-file .env.local python -m my_agent.worker
```

The console is prebuilt in the Python package; no Node.js or frontend build is needed. `dev` runs
the session-manager worker and the API/console server in one process; `session-manager` and
`serve` remain available as separate commands for deployments. Defaults are `localhost:7233` and
namespace `default`. For another Temporal service, export the same `TEMPORAL_*` connection
settings in each service terminal. `.env.local` above loads only into the agent worker. Keep
`TASK_QUEUE_PREFIX` consistent across services. Keep all three processes running.

### 4. Verify, then develop with your coding agent

1. Open the **harness console at <http://localhost:8000>**. Create a session for
   **OpenAI Hello (Python)**.
2. Send a `ping` message. The message box's handler button names the handler your
   message goes to; a new session starts on `ping`. Type any text, such as `hello`,
   and press Enter. Expect the same text back without a model request or an API key.
3. Send an `ask` message. This step calls OpenAI:
   1. Set `OPENAI_API_KEY` in `.env.local`.
   2. Restart the worker so it reads the key, which it loads only at startup: in Terminal 3,
      press Ctrl+C, then run `uv run --env-file .env.local python -m my_agent.worker` again.
      Leave the other services running; your session picks up where it left off once
      the worker is back.
   3. Switch the message box to `ask`: click the handler button (it shows `ping`) and choose
      `ask`, or type `/` in the message box and pick `ask` from the list. The button now shows
      `ask`, and later messages go to `ask` until you switch again.
   4. Send **“What's the weather in Paris?”**. Expect a streamed reply and a completed
      `get_weather` tool trace with sample weather data.
4. Reload the page and confirm the session reconnects.
5. Check configuration and service availability:

   ```bash
   uv run temporal-agent-harness doctor --registry agents.toml
   ```

Add `--json` for a machine-readable report. The doctor checks the registry and schemas, Temporal
connectivity, task queue pollers, and the API without creating or resetting sessions. A failed
check exits 1. Temporal's own workflow UI is at <http://localhost:8233>; it is separate from the
harness console.

With the services running, open the project in your coding agent. Reload its session if it was
already running, then ask it to use the skill:

| Tool | Prompt |
| --- | --- |
| Codex | `Use $temporal-harness-python to help me build this agent. Explain the starter, then verify it with ping before making model calls.` |
| Claude Code | `/temporal-harness-python Explain the starter, then verify it with ping before making model calls.` |

Edit `my_agent/workflow.py` to change your agent and `my_agent/worker.py` for worker configuration.
After changes, stop the worker, regenerate the schema with the command in step 2, and restart
it with the command in step 3. Use a new session for changed workflow behavior.
Commit `agent.schema.json` and `uv.lock`.

After upgrading the SDK, rerun the `skills install` commands to update unchanged skill files.
Local edits produce a conflict and are preserved. Application-client guidance is in the installed
skill's `references/api.md`.

See [coding-agent development](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/agent-development.md)
for skill discovery and updates. The `init`, `skills`, and `doctor` commands ship with the base
package; `dev`, `serve`, and OpenAPI export require the `ui` extra.

<a id="installation"></a>

## Installation and manual reference

For a new application, start with the [four-step quickstart](#quickstart) above.
The reference below covers existing projects, model integration extras, and repository examples.

### Try the example agents

Clone at a release tag. You need [just](https://just.systems/),
[Bazelisk](https://github.com/bazelbuild/bazelisk) installed as `bazel`, and the
[`temporal` CLI](https://docs.temporal.io/cli) (see [Requirements](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/guides/examples.md#requirements)).

```bash
git clone --branch 0.11.0 https://github.com/temporal-community/temporal-agent-harness.git
cd temporal-agent-harness
cp .env.example .env.local   # then set GEMINI_API_KEY (and/or OPENAI_API_KEY)
```

Then run two commands in separate terminals to get going with all example agents:

```bash
just temporal       # 1. local Temporal dev server (needs the `temporal` CLI)
just dev            # 2. session-manager, harness server and all example agent workers
```

Open <http://localhost:8000> and start a session. There's no install step — the recipes build with
Bazel, which fetches dependencies on first run. Every example follows the same four recipes; see
[Run the examples](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/guides/examples.md#run-the-examples) for the rest of them, including running all eight behind
one UI.

Git will note that you're in "detached HEAD" — that's expected, it just means you're sitting on
the tag rather than on a branch. Later, move to a newer release with
`git fetch --tags && git checkout <version>`, or see what changed between two of them with
`git diff 0.10.0 0.11.0`.

### Build with it — install from PyPI

Use this path to build your own agents in your own project. You don’t need to clone this repository.

The harness is published to
[PyPI](https://pypi.org/project/temporal-agent-harness/). In a
[`uv`](https://docs.astral.sh/uv/)-managed project:

```bash
uv add 'temporal-agent-harness[ui]==0.11.0'
```

Or declare it in `pyproject.toml` — an ordinary dependency, no `[tool.uv.sources]` needed:

```toml
[project]
dependencies = [
    # Everything past the base install is an opt-in extra — combine as many as you need
    # in the one spec, e.g. [ui,code-mode,genai]:
    #
    #   ui              the browser UI/API server dependencies
    #   code-mode       the sandbox a worker runs Code Mode scripts in
    #   genai           the Google Gemini integration
    #   openai-agents   the OpenAI Agents SDK integration
    #   pydantic-ai     the Pydantic AI integration
    #   strands-agents  the Strands Agents integration
    #   typesafe        typed decisions and the Jev auto mode evaluator, via the canonical
    #                   temporalio-typesafe plugin
    #   s3              S3-backed offload for large payloads
    #
    # What each one pulls in, and when you actually need it, is in the Extras table below.
    "temporal-agent-harness[ui]==0.11.0",
]
```

Then run `uv sync`. Pin the exact version: this project is pre-1.0 and APIs change between
releases — see [Versioning and stability](#versioning-and-stability).

#### Run the web UI — no checkout, no Node

The package installs a `temporal-agent-harness` command; the `ui` extra adds its server runtime —
`uv run` it, the way you would any other project tool. It serves the browser UI built into the
wheel: nothing to clone, nothing to build. One command per terminal:

```bash
# 1. A local Temporal dev server (needs the `temporal` CLI). Its own Web UI: http://localhost:8233
temporal server start-dev

# 2. The packaged session-manager worker. It hosts SessionManagerWorkflow, which starts your
#    agents as child workflows — without it the server comes up but has nothing to answer
#    /api/agents or session creation, since both are workflow queries.
uv run temporal-agent-harness session-manager

# 3. The prebuilt Svelte UI + JSON API on http://localhost:8000
uv run temporal-agent-harness serve ./agents.toml

# 4. Your own worker, hosting your agent workflows on their own task queue
uv run python -m my_app.worker
```

Open <http://localhost:8000> and create a session. `serve` accepts several registries and merges
them (`serve one/agents.toml two/agents.toml`), so a single UI can list agents from more than one
project. From the next release, `temporal-agent-harness dev ./agents.toml` runs steps 2 and 3 in
one process, as the quickstart does.

That registry is what lists the launchable agents:

```toml
[[agents]]
key = "my-agent"
workflow_type = "MyAgent"
task_queue = "my-agent-task-queue"
label = "My Agent"
description = "A short description shown in the UI."
schema = "MyAgent.schema.json"   # optional; see below
```

`schema` is the agent's schema document, relative to the registry, which the
`schema` subcommand writes from its class:

```bash
uv run temporal-agent-harness schema my_app.workflow:MyAgentWorkflow -o MyAgent.schema.json
```

The server reads what the agent takes to start from it and never imports the agent's code. If its
`@agent.init` takes a `data` model after its config, required (`data: MyData`) or optional
(`data: MyData | None = None`), `GET /api/agents` publishes it and the console asks for it in a
form before starting a session. `POST /api/sessions` answers `422` when `data` is missing for an
agent that requires it or sent to one that takes none; the agent validates the data itself. Without
`schema`, any `data` a caller sends goes through for the agent to validate. Regenerate the schema
when the agent's init data changes. (This repo's examples have Bazel generate each `agents.toml`
and its schemas; see `bazel/agents.bzl`.)

Both subcommands resolve their Temporal connection through temporalio's standard client config,
so they land on the same namespace. With nothing configured they use `localhost:7233` — the
`start-dev` default. Note that a `temporal.toml` in the **current directory is not picked up
automatically**; point at a project-local file (or a Temporal Cloud profile) explicitly:

```bash
TEMPORAL_CONFIG_FILE=./temporal.toml TEMPORAL_PROFILE=cloud \
    uv run temporal-agent-harness serve ./agents.toml
```

`uv run temporal-agent-harness --help` documents the full precedence order.

Need the API and UI mounted inside your own FastAPI service instead? See
[Self-hosting the web app](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/api/hosting.md#self-hosting-the-web-app).

#### Extras

Agent authors import the runtime from `temporal_agent_harness.harness`; the base install carries
only `temporalio` and `pydantic`, so nothing drags in an AI SDK you don't use. Everything else is
opt-in:

| Extra | Add it when you… |
| --- | --- |
| `ui` | want the browser UI and HTTP API server (pulls in `fastapi[standard]`, including Uvicorn). The built Svelte assets are always in the wheel; only the server runtime is gated here, so agent-worker installs stay small. |
| `code-mode` | run a worker that hosts **Code Mode** agents; pulls in [`pydantic-monty`](https://pypi.org/project/pydantic-monty/), the sandbox the scripts run in. Importing `agent` needs nothing extra; building a `code_mode_tool` does. |
| `genai` | use the **Google Gemini** integration (`ai_sdks.google_genai_plugin`). |
| `openai-agents` | use the **OpenAI Agents SDK** integration (`ai_sdks.openai_agents`). |
| `pydantic-ai` | use the **Pydantic AI** integration (`ai_sdks.pydantic_ai_harness`). |
| `strands-agents` | use the **Strands Agents** integration (`ai_sdks.strands_agents`, with the harness glue in `ai_sdks.strands_agents_harness`); pulls in `strands-agents` and the MCP packages its `TemporalMCPClient` uses. |
| `typesafe` | make typed [TypeSafe](https://docs.typesafe.ai) System One calls (`ai_sdks.typesafe_harness`), or use **`ai_sdks.jev.jev_evaluator`**, the Jev-backed auto mode evaluator; pulls in [`temporalio-typesafe`](https://pypi.org/project/temporalio-typesafe/). Register its `TypeSafePlugin` on the worker, before `AgentHarnessPlugin`. |
| `s3` | offload large payloads to S3. The default local-filesystem driver needs nothing extra. |

Combine them in one spec, e.g. `uv add 'temporal-agent-harness[ui,code-mode,genai]==0.11.0'`.


## Versioning and stability

**Pin to a release.** Every release is listed on the
[releases page](https://github.com/temporal-community/temporal-agent-harness/releases) and
published to [PyPI](https://pypi.org/project/temporal-agent-harness/). A release tag marks a
commit that is known-good at the moment it was cut: the tests passed, and the published wheel and
sdist were built from that commit, browser UI included.

Two artifacts come out of a release, and they pin differently:

- **The library** — pin an exact version from PyPI (`temporal-agent-harness==0.11.0`).
- **The examples** — check out the matching git tag. They are *not* shipped in the package, so a
  PyPI install gives you the library, the packaged UI, and the CLI, but no `examples/` tree.

**This project is very early and experimental - `main` may break at any time.**

Releases are cut when the maintainers judge the state stable ([how](https://github.com/temporal-community/temporal-agent-harness/blob/main/RELEASE.md)). This project is
**experimental** and pre-1.0: APIs will change between releases, without warning. Pinning is what
keeps that churn from reaching you unannounced — so pin, and upgrade deliberately when you want
to try out new harness capabilities.


## A taste

```python
from datetime import timedelta

from pydantic import BaseModel
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.workflow import ActivityConfig

from temporal_agent_harness.harness import AgentWorkflowRunner, agent
from temporal_agent_harness.harness.agent_protocol import AgentConfig, MidTurn, ToolApprovalPolicy


# A durable, activity-backed tool: runs as a retried, observable Temporal activity and
# publishes its own tool_start/tool_end events on the turn stream.
@agent.activity_tool_defn(
    activity_config=ActivityConfig(start_to_close_timeout=timedelta(seconds=30)),
)
async def search_flights(origin: str, destination: str, date: str) -> str:
    ...


# Strongly-typed messages — an agent operation is more than a string in and a string out.
class PlanTrip(BaseModel):
    destination: str
    nights: int


class Itinerary(BaseModel):
    summary: str
    total_usd: float


# The agent's own init data, passed after the AgentConfig when a session starts.
class TravelerProfile(BaseModel):
    """Who the agent is planning for."""

    name: str
    home_airport: str


# @agent.defn makes the class a Temporal workflow and provides its run method, which drives
# the turn loop. Add an optional `@agent.setup` / `@agent.teardown` method for one-time async
# work before the first message or after the agent closes.
@agent.defn
class TravelAgent:
    @agent.init
    def __init__(self, config: AgentConfig, profile: TravelerProfile | None = None) -> None:
        # Tool approvals are safe-by-default; here, auto-approve only tools that
        # statically declare themselves inherently safe.
        self._runner = AgentWorkflowRunner(
            config,
            stream=WorkflowStream(),
            approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
        )
        # Optional, so a caller may start it without a profile. Declare it without the
        # `| None = None` to make it required.
        self._profile = profile

    # A typed, self-describing operation. The agent advertises this signature, so callers —
    # your code or another agent — can drive it programmatically. Your turn logic goes here:
    # call your AI SDK, run tools through the runner, and return the typed reply.
    # (See examples/monty for a complete, model-in-the-loop agent on the Gemini integration.)
    @agent.accepts
    async def plan_trip(self, request: PlanTrip) -> Itinerary:
        ...
```


### Closing idle agents

An agent's workflow stays open between messages until something closes it. While it waits for
the next message the agent is *idle*: no turn is running, and it consumes no compute. A Temporal
workflow that is waiting is durable state on the server, not a process spinning on a worker.
Staying open is what lets a conversation pick up where it left off, hours or days later.

Some agents are of no use once their user stops interacting with them. Take the agent behind a
chat box on a web page whose session is not kept: when the user leaves, nothing will ever send
it another message. For an agent like that, give the runner an idle timeout, and it closes the
agent once no one has talked to it for that long:

```python
self._runner = AgentWorkflowRunner(
    config,
    stream=WorkflowStream(),
    approval_policy_default=ToolApprovalPolicy.allow_inherently_safe(),
    close_after_idle_default=timedelta(minutes=30),
)
```

In TypeScript, it is `defineAgent({ closeAfterIdleDefault: "30 minutes", ... })`.

Once the agent has been idle for that long, the runner closes it the same way a `close` signal
does:

- `@agent.teardown` runs;
- the stream's last event is `agent_closed` with reason `idle_timeout_expired`, and
  `agent_status` reports the same `close_reason`;
- a message sent afterwards is refused with `AgentClosed`.

Only idle time counts. A running turn is never idle, even one that waits hours on a tool call,
a human approval, or a subagent; the clock starts again from zero each time a turn ends.

A caller can override the agent's default when it starts a session, with
`AgentConfig(close_after_idle=...)`; `timedelta(0)` means never close on idle. If the user
comes back after their agent has closed, `AgentClient.start_and_submit_message` starts a new
session under the same workflow id. It starts fresh, without the closed session's conversation.

> **Subagents never close on idle.** This is deliberate: a parent owns the whole lifecycle of
> its subagents. It may leave a subagent waiting between messages for as long as it likes, and
> it alone decides when that subagent ends, with `stop_<key>`. So whenever a parent starts a
> subagent, the harness turns the subagent's idle timeout off, overriding any default the
> subagent's own agent sets.

## Running a worker — one plugin

An agent is a Temporal workflow, so it runs on a Temporal worker. `AgentHarnessPlugin` is the
single registration that wires that worker (and its client) for the harness — you never
assemble the harness's activity list or its data converter by hand:

```python
from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from temporal_agent_harness.plugin import AgentHarnessPlugin

client = await Client.connect(
    **ClientConfig.load_client_connect_config(),
    # Your AI SDK's plugin first, the harness plugin last.
    plugins=[OpenAIAgentsPlugin(model_params=...), AgentHarnessPlugin(tools=MY_TOOLS)],
)

worker = Worker(client, task_queue="my-agent", workflows=[TravelAgent])
await worker.run()
```

The worker declares only its workflows. Adding the plugin brings:

- the harness's **data converter** (Pydantic + large-payload offload) — add the plugin to every
  client, worker, and server in the deployment so they all agree on it;
- the durable **activity body** of each `@agent.activity_tool_defn` tool in `tools=` (pass your
  whole toolset — tools with no worker-side body are skipped);
- the **subagent** and **Code Mode** activities, and the two **Stream** Local Activities
  through which the Workflow code of a `stream=ContribStream()` agent creates and writes its
  Stream.

Order it last, after any AI SDK's plugin, so that SDK's payload converter wins. Registering it
on the client is enough — Temporal applies a client's plugins to workers built from it.

## Choosing the event stream

Each agent picks where it publishes its turn events, with the required `stream=` of
`AgentWorkflowRunner`. There's no default, so one deployment can run agents on both kinds.
`stream=WorkflowStream()` keeps the events in the workflow's own history with
`temporalio.contrib.workflow_streams`. `stream=ContribStream()` puts them on a Stream of the
SDK's Streams API, in a Stream Store such as Redis. Any other `StreamBackend` with `open_sink()`
works too.

```python
self._runner = AgentWorkflowRunner(config, stream=WorkflowStream(), approval_policy_default=...)
self._runner = AgentWorkflowRunner(config, stream=ContribStream(), approval_policy_default=...)
```

A `ContribStream` deployment has the same Stream Store on every client. Workers take their
client's Store.

```python
from temporal_agent_harness.harness.contrib_stream import stream_stores_from_env

client = await Client.connect(
    ..., plugins=[AgentHarnessPlugin()], stream_stores=stream_stores_from_env()
)
```

`stream_stores_from_env()` reads `STREAMS_STORE` (`redis` or `memory`), `STREAMS_REDIS_URL`
and `STREAMS_KEY_PREFIX`. It gives a list with a `RedisStreamStore`
(`temporalio.contrib.redis.streams`) or a `MemoryStreamStore` (`temporalio.testing`). With
`STREAMS_STORE` unset the list is empty. The web app and the Nexus adapter add the same Store
when `STREAMS_STORE` is set.

The end of an agent's run closes nothing, and the harness does not close the Stream either. A
reader of an agent that ended stops once it finds that the agent no longer runs. The Store keeps
an open Stream until its `last_write_ttl` after the last write. The Store that
`stream_stores_from_env()` gives sets none, so build a Store of your own to set one.

A root agent gives out its Stream's place, in a reply, a subagent or an Activity, only after the
create of the Stream returned. A status can't wait, so it names the Stream from the start. A
reader of a Stream that doesn't exist yet waits for it while the agent runs.

Workflow code can't do I/O, so it writes the Stream through two Local Activities of the harness.
`agent_harness.create_event_stream` creates the root's Stream, or finds it on a later run of the
chain. `agent_harness.append_events` writes a batch of events. Both use the client's Streams API
through `activity.client()`, and `AgentHarnessPlugin` registers them. The Workflow sends one
append at a time. Each append takes every event that waits, so a burst of events is one append,
and the events keep their order. An Activity buffers its events and writes them at most
`batch_interval` apart, and also when it fails.

`ContribStream` needs an SDK build with the Streams API. `just install-streams-sdk` installs the
one the harness is tested with. The harness imports the Streams API only for a `ContribStream`
agent, so an agent on `stream=WorkflowStream()` runs on a released SDK. On a Worker whose client
has no Store, the Local Activity that creates the root's Stream fails as non-retryable with what
to pass, and so does the run. A client or an Activity with no Store raises a `RuntimeError` that
says what to pass. The examples pass `stream=WorkflowStream()`. An agent written with the
TypeScript SDK publishes on Workflow Streams (see `typescript/docs/python-parity.md`).

The rest of the harness is the same on both. A client reads an agent through `AgentClient`, the
web app or the Nexus adapter, and the agent says where it publishes (`AgentStatus.stream`). A
subagent's answer comes back from the child's `await_message` update, not from its events, so a
lost event can't lose a subagent's result. Resume points are opaque strings on both.

A subagent publishes where its parent does, whatever its own `stream=` says. On `ContribStream`
the whole agent tree shares the root's Stream, and a reader keeps one agent's events by their
`agent_id`. On Workflow Streams each agent has its own log. An agent that continues as new
passes its Stream on with `workflow.continue_as_new(self._runner.continue_as_new_config(config))`.

| | `ContribStream` | Workflow Streams |
| --- | --- | --- |
| What it needs | A Stream Store, the same Store on every process, an SDK build with the Streams API | Temporal only |
| Where events go | Into the Store, until its `last_write_ttl` after the last write, since nothing closes the Stream | Into each workflow's own log, in its history |
| Temporal Cloud Actions | All Local Activity writes of one Workflow Task count as one Action, and each Workflow Task heartbeat adds one. Activity writes and client reads go to the Store | Each activity flush is a Signal and each client read an Update, one Action each |
| History | One Local Activity marker per append, with no event payloads | The Signals and Updates, with their payloads |
| Subagents | One Stream per tree in one order. A resume point advances on every event | One stream per agent, which the client merges. A resume point advances on root events only |
| A retried attempt | A reader gets `attempt_superseded` with the offsets of the earlier attempt's records, of an Activity or of a Workflow Task | Its events show twice |
| A Store outage | The Local Activity of each Workflow append retries with no limit, and the end of a run waits for its appends. So events come late, and the run can't finish until the Store is back | No Store to fail |

A Nexus caller of `pollMessages` can't apply `attempt_superseded` on `ContribStream`. Each item
there has `offset` 0, so the caller can't tell which items the event withdraws. The Go driver in
`nexus/ui_connector/agent` skips the event, so a retried model call shows the text of both
attempts.

Changing an agent's `stream=` changes its workflow code, so drain or terminate its open sessions
first, or deploy it with worker versioning. Upgrade the workers before the clients, since the
client reads `AgentStatus.last_event_seq` and `AgentEvent.seq` from the worker.


## Further reading

- [Harness API and session lifecycle](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/api/README.md)
- [Messages and handler behavior](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/guides/messages.md)
- [Approvals and auto mode](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/guides/approvals.md)
- [Code Mode, files, and subagents](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/guides/code-mode.md)
- [Build a React or Svelte UI](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/guides/client-ui.md)
- [Run the repository examples](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/guides/examples.md)
- [Embed the web app in FastAPI](https://github.com/temporal-community/temporal-agent-harness/blob/main/docs/api/hosting.md)
