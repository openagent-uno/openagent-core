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
signature through the public `FastMCP.call_tool()` API. The adapter deliberately
does not pre-validate inputs against JSON Schema: FastMCP disables that low-level
validator to preserve its own argument conversion and validation messages.
The SDK converts handler results; the adapter preserves the complete
`CallToolResult` in `ToolResult.mcp_result`, including structured output, media,
error flags, null fields, metadata and extension fields. It validates structured
output against the published output schema, as the MCP server does. Ordinary
catalog discovery, references, per-call authorization,
revocation, family allowlists and the deployment denylist still apply. Handler
results and errors keep their existing semantics. Provider discovery/probe tools
still contact their provider when an authorized caller requests them; enabling
the transport itself does not contact a provider.

Published function names use the same `MCPPool._safe_prefix(source) + "_"`
convention as stdio, for example `model_manager_list_providers`. Only the
internal call to FastMCP keeps the original `list_providers` handler name.
Original module handlers are not renamed or patched. Contract comparisons must
compare the actual published names and schemas without stripping prefixes.

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

The first isolated Linux wire probe found that the original opt-in returned a
raw handler value instead of the MCP envelope. The first envelope correction
passed that probe and 33 of 34 focused tests, but failed the exact SDK invalid
argument comparison because it added JSON-schema input pre-validation. Both
failed runs remain unaccepted. `tests/public/test_management_wire_envelopes.py`
adds exact whole-result comparisons through a real SDK protocol session,
including invalid input, handler errors, structured results, media and metadata.
These tests do not use the domain-payload accessor in the lifecycle test file.

After removing only the redundant input pre-validation, Linux qualification
passed the exact SDK wire probe and **34/34 Core tests**, with no skips, in
6.187 seconds on Python 3.12.15 and MCP 1.30.0. Kernel cgroup memory peak was
101,490,688 bytes, with no memory-limit or OOM events. The isolated unit used
one CPU, 1536 MiB, no swap, nice 19 and a 600-second limit. Synthetic data and
logs used private 128 MiB tmpfs; source and closure were read-only, with exact
read-only masks, no capabilities, private networking and effective ingress and
egress BPF denial. Original source hashes, factory receipt and production
PID/start time remained unchanged. No Mac tests were rerun.

The subsequent five-Runtime stdio comparison stopped during worker imports:
host CPU PSI avg10 reached 26.30%, exceeding the unchanged 20% guard. Memory
and I/O PSI avg10 were zero; cgroup peak was 211,095,552 bytes. No idle result
was obtained, and the opt-in phase was not started. This is an unaccepted
measurement, not a functional regression or evidence of net memory savings.

A later single-Runtime probe first exposed a real discovery naming mismatch;
the opt-in now uses the stdio prefixes described above. Its failed run is
preserved, including the sampler's process-exit race. The corrected sampler
ignores a disappeared PID only during close, after proving it belonged to this
cgroup and either became a zombie with the same start time or disappeared from
both `/proc` and the cgroup. Reused PIDs, permission failures and unexpected loss
during idle fail the test.

After that correction, a separate serial Linux comparison passed for **one
Runtime and three management sources per transport**. Each phase first passed
the same 34 focused tests and raw SDK probe. The complete actual discovery
names, input schemas and descriptions (27 tools), and the three exercised
Runtime result envelopes, matched exactly; the stdio reference was read-only
in the opt-in phase. Both used equivalent synthetic DBs and policy, five seconds
of fixed warm-up and five seconds of idle sampling. Thirty later calls were
timed after discarding the three warm-up calls.

Median parent-plus-children idle PSS was **269,126,656 bytes** for stdio and
**107,147,264 bytes** for opt-in: an observed synthetic delta of **161,979,392
bytes (154.5 MiB)** for this one Runtime. The counts were five processes
(measurement process, sandbox coordinator and three stdio workers) versus two.
Both measurement processes had the same focused-suite imports loaded. Cgroup
idle anonymous memory was 252,604,416 versus 91,013,120 bytes; idle file memory
was 3,579,904 versus 3,190,784 bytes. Warm call median/p95 was 31.68/78.77 ms
versus 16.61/35.95 ms under the same one-CPU quota. All owned children drained,
and postflight again preserved original hashes, factory receipt and production
PID/start time. Limits and guards were unchanged.

This single-instance synthetic observation does not replace the refused
five-Runtime measurement, qualify a released bundle, or establish reclaimed
production memory. Do not multiply it by the number of hosted agents.

Run the Core suite from this checkout with the configured source paths:

```sh
.venv/bin/python -m pytest -q tests/public/test_management_in_process.py tests/public/test_management_wire_envelopes.py
```

This is source qualification, not a released wheel or an installed native
bundle qualification. No package version, pin or release was changed. Before
host activation, qualify a derived exact bundle on Linux with the host's
normal storage/ACL and sandbox admission; compare stdio versus opt-in on the
same workload, DB fixture and reserve. Record PSS, cgroup anon/file/peak,
tool latency, cancellation and idle child counts. The previous fifteen idle
workers define an optimization scope, **not measured net savings**. Accepted
reclaimed RAM remains zero until that comparison passes.
