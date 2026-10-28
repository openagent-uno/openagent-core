# Opt-in in-process management transport

This source change adds a host-selected transport for `model-manager`,
`budget-manager` and `skill-data`. `NativeCapabilityDescriptor` appends the
strict boolean `in_process_management=False`; existing positional arguments,
official module defaults and standalone stdio selection are unchanged. A host
can clone an installed descriptor with
`dataclasses.replace(descriptor, in_process_management=True)`. Other sources in
the same descriptor keep their existing transport.

The selected module must provide an absolute `options.db_path`. An explicit
`OPENAGENT_DB_PATH` in its environment must agree with that path. There is no
ambient database fallback for this transport. Native module source names,
aliases, effects, catalog registration and host authorization remain on the
same path; the change does not move native modules into the user MCP catalog.
The pool also accepts this exact opt-in for its three builtin entries and
awaits asynchronous toolkit factories.

Each toolkit owns its lazy `MemoryDB` connection (models and budget), active
calls and close state. Skill-data retains its original read-only query handlers
and per-call connections. Original handler functions are reused without
patching their globals or cloning imported modules. A task-local binding carries
only the owning toolkit and task; it is reset in `finally`, rejects an inherited
background-task binding, and verifies the owning Runtime when one was bound at
construction. No process environment or provider credential map is copied into
the adapter. Legacy stdio handlers still use their own legacy connection when
there is no binding, through a lightweight context module.

Toolkits take their exact published input schemas and descriptions from the
public `FastMCP.list_tools()` API. Pydantic validates the preserved callable
signature. Ordinary catalog discovery, references, per-call authorization,
revocation, family allowlists and the deployment denylist still apply. Handler
results and errors keep their existing semantics. Provider discovery/probe tools
still contact their provider when an authorized caller requests them; enabling
the transport itself does not contact a provider.

Pool close and replacement close the owned toolkit, deny new calls, cancel and
drain only its active calls, and close its database. A failed or cancelled pool
replacement also closes the now-unreachable previous instance. The module
startup failure path closes its partial pool. No scheduler, worker, browser,
computer, vault, model selection, tokenizer cache or historical identity changes.

## Source qualification and remaining gates

`tests/public/test_management_in_process.py` contains 28 focused tests against
real synthetic SQLite databases, the real SDK schema API, real MCP pools and
two concurrent public Runtime instances. The companion ICS consumer has five
source tests. The final combined run on 28 October 2026 passed **33/33**, no
skips, in 3.33 seconds including imports, with **112,672,768 bytes** maximum RSS
reported by the process (external sampled peak: **115,441,664 bytes**).
It used Python 3.11.6, MCP 1.30.0, aiosqlite 0.22.1 and Pydantic 2.14.0 already
installed locally. Network connections were forbidden and HOME/TMP were private.
No real provider, credentials, DB or native engine were used. The test process
ran nice 19 with a 45 CPU-second limit and an external 256 MiB RSS/60 s wall
guard; macOS supplied no cgroup CPU quota. Earlier fixture/schema failures are
retained in the private qualification logs.

Run the Core suite from this checkout with the configured source paths:

```sh
.venv/bin/python -m pytest -q tests/public/test_management_in_process.py
```

This is source qualification, not a released wheel or an installed native
bundle qualification. No package version, pin or release was changed. Before
host activation, qualify a derived exact bundle on Linux with the host's
normal storage/ACL and sandbox admission; compare stdio versus opt-in on the
same workload, DB fixture and reserve. Record PSS, cgroup anon/file/peak,
tool latency, cancellation and idle child counts. The previous fifteen idle
workers define an optimization scope, **not measured net savings**. Accepted
reclaimed RAM remains zero until that comparison passes.
