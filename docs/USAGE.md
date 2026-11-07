# Using Core, the engine, gateway and SDK

Read [components](COMPONENTS.md) and [agnostic architecture](AGNOSTICISM.md).

## Reproducible engine qualification

Use a clean checkout with Python 3.11+, Node/npm and `uv==0.11.14` available in
the build toolchain. No standalone product or existing agent environment is
needed. Output and environment directories must be new, task-owned directories:

```sh
python packaging/build.py --out /path/to/new-wheel-directory
python packaging/qualify.py --wheels /path/to/new-wheel-directory --environment /path/to/new-environment
```

The runner checks candidate wheel hashes/metadata and exact source files before
creating an isolated environment. It installs the entire candidate cohort,
including independently versioned optional modules, and checksum-pinned public
Tools contracts. It checks dependency consistency and the isolated Python import
versions, installs the SDK lockfile, then runs Node SDK and Python public/prompt
tests. `installed-environment.json`, `public-engine-tests.xml` and
`engine-qualification.json` record the actual scope. A receipt is written only
after all commands succeed. `--allow-dirty-source` is a development-only opt-in;
it never records release qualification. Existing environments are never modified.

Do not mix a source checkout with an older installed Core: audio workers use
`python -I`, deliberately ignoring source `PYTHONPATH`. Likewise, do not replace
the SDK compiler with a global `tsc`; run `npm --prefix sdk/typescript ci`.
Version mismatches are preparation failures, not evidence of a current runtime
regression. This gate performs no model inference or real messaging-provider IO.

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

### Minimal SQLite dependency boundary

SQLite imports and synchronous migration/privacy guards do not load the optional
engine. Select the engine extra only for reasoning or asynchronous memory APIs.
The isolated import regression runs with Python site packages disabled and
checks that protected snapshots remain fenced.

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

## Engine-free SQLite import

The SQLite adapter no longer eagerly loads MemoryDB or aiosqlite through
migration/privacy helpers. Install the engine extra only when selecting the
reasoning engine or its asynchronous memory API.

- [Optional remote capabilities, inventory and media](remote-capabilities.md)
