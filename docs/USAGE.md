# Using Core, the engine, gateway and SDK

Read [components](COMPONENTS.md) and [agnostic architecture](AGNOSTICISM.md).
Python examples require **Python 3.11+**. Exact dependency versions come from the
reviewed wheel manifest and each distribution's `pyproject.toml`, not a moving
README version. A source candidate is not automatically a published release.

## Minimal in-process kernel

Install the exact compatible `openagent-core` and `openagent-storage-sqlite`
release set into an isolated environment. No standalone server, network identity,
provider key or computer tool is needed for this demo. The kernel itself has no
third-party dependencies; the selected storage adapter has its own import boundary.

Run [examples/minimal-runtime.py](../examples/minimal-runtime.py). It constructs
an explicit principal/context, a policy, a SQLite store and a custom echo
executor, then starts Runtime, submits a run, waits and closes it. The database
lives in an owned temporary directory, which the example removes. It does not
contact a model or represent a real identity provider.

### Current source limitation: SQLite import coupling

In the reviewed source candidate, importing `SqliteRuntimeStore` reaches migration
helpers through `openagent_core.memory`, whose package initializer imports
`MemoryDB` eagerly. A bare Python 3.11 environment therefore reports missing
`aiosqlite`; the adapter currently needs the engine dependency set even when this
demo selects no reasoning engine. This is generic upstream packaging debt, not
a reason to patch the consumer or install a standalone server. Correct the lazy
import/dependency boundary upstream and qualify the exact wheel set before
claiming a dependency-minimal SQLite installation. The example was executed in
the existing Core development environment with those dependencies available.

From a source checkout on a POSIX shell, use a Python 3.11+ interpreter in that
prepared development environment and these source paths:

```sh
PYTHONPATH=packages/runtime/src:packages/storage-sqlite/src \
  python3 examples/minimal-runtime.py
```

Expected output: `hello from a generic runtime`. This verifies wiring with the selected adapter, not model
reasoning or production authorization. Production hosts construct contexts only
after authentication and supply their actual policy; never trust caller JSON for it.

## Enable the reasoning engine and optional modules

Select `openagent-core[engine]`, adding only required provider/retrieval/audio
extras and module wheels from the verified release set. Supply a configured
`Agent`, storage/model services and explicit capability sources, then wrap the
agent with `openagent_core.engine.AgentExecutor` as the Runtime executor.
The lifecycle/admission boundary stays the same as the demo. Engine bootstrap
details are in [architecture](architecture.md); a complete standalone composition
is in `openagent/apps/server`, not a code fragment to copy into an embedded host.

Use `RuntimeProfile` plus a ModuleCatalog to select module IDs/surfaces. Installing
Vault, Workflows or MCP does not enable it; omitting a module removes its tools,
workers and instructions. Use [the module contract](modules-v1.1.md) and the
individual package READMEs rather than adding product feature switches to Core.

## Optional remote run API

Install the exact `openagent-gateway` wheel and its dependencies. Trusted host
code supplies `resolve_context` and calls:

```python
from openagent_gateway import create_app

# runtime and resolve_context are supplied by the authenticated host.
app = create_app(runtime, resolve_context)
```

`create_app` builds routes; the host explicitly starts its aiohttp listener.
Its default lifecycle integration starts/closes Runtime with the app. If Runtime
is already managed elsewhere, select `manage_lifecycle=False` and keep one owner.
The resolver receives `(request, action, run_id, session_id)` and yields a verified
ExecutionContext. Verify attachment references through `resolve_attachments`
when that route admits attachments.

| Operation | Default HTTP route |
| --- | --- |
| Submit | `POST /api/v1/runs` |
| Read current state | `GET /api/v1/runs/{run_id}` |
| Replay after cursor | `GET /api/v1/runs/{run_id}/events?after={cursor}` |
| Cancel exact run | `POST /api/v1/runs/{run_id}/cancel` |
| List children | `GET /api/v1/runs/{run_id}/children` |

## Python and TypeScript remote clients

The Python SDK supplies `openagent_sdk.RunClient`. In an authenticated async
host/client integration:

```python
from openagent_core import RunRequest
from openagent_sdk import AcceptanceUncertain, RunClient

# These are provided by the embedding integration, not literals in this guide.
client = RunClient(runtime_origin, authenticated_headers)
request = RunRequest(run_id, session_id, idempotency_key, "Summarize this input")
try:
    record = await client.submit(request)
except AcceptanceUncertain:
    record = await client.get(request.run_id)
result = await client.wait(record.run_id)
events = await client.events(record.run_id, after=0)
```

This is an integration fragment, not a standalone authentication implementation.
If reconciliation returns not-found while admission remains uncertain, follow
the host's bounded same-request reconciliation policy; do not allocate a second
run ID or blindly retry a tool effect. Persist replay cursors and inspect terminal status.

The TypeScript package `@openagent/sdk` exports the same `RunClient` operations;
its header callback may be asynchronous. See [Python source](../sdk/python/src/openagent_sdk/__init__.py),
[TypeScript source](../sdk/typescript/src/index.ts) and the
[authenticated gateway fixture](../tests/public/test_gateway_sdk.py).
SDK use is separate from installing OpenAgent App or CLI and from model-provider SDKs.

## Tools without a reasoning runtime

`openagent-capability-host` is an optional, engine-free host for explicit tool
providers and MCP processes. Configure its paths, providers and consent; it does
not install the tools or authenticate the application's users. Tools may also
be called directly without this host. See its [README](../packages/capability-host/README.md)
and the [Tools usage guide](https://github.com/openagent-uno/openagent-tools/blob/main/docs/USAGE.md).
