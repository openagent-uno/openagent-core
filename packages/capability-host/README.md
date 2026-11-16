# Capability host

Optional engine-free discovery and execution host. Construction receives explicit paths and capabilities, does not create files, and installs no built-in tools. Plugin loading is disabled unless the host product enables it. Per-principal durable idempotency, admission/revocation barriers, leases and lossless results retain the existing implementation.

Extracted from openagent-host-tools af6ad6871d4d1208874bf79735710d089f59b959. Public tool value types are provided by the independent openagent-tool-protocol package; there is no dependency on the runtime or concrete tools.

The stdio adapter reads complete bounded MCP catalogs and refreshes them on
`notifications/tools/list_changed`, bound to the same child process. Failed
refreshes remove stale definitions and publish an unavailable state. See
[the unreleased source contract and qualification](../../docs/mcp-catalog-refresh.md).

## On-demand processes

`MCPStdioServer(..., idle_timeout=seconds)` stops a ready child after that many
seconds without a call or resource read. The catalog read at the last start stays
published, so hosts keep offering the tools to the model while no process runs.
The next call starts the child again under the same spawn lock (one process for
concurrent callers), repeats `initialize` and `tools/list`, and reports a state
change only if the catalog differs. A call that is still running holds the
process; every call or read restarts the idle period. An idle stop is an expected
exit: it consumes no restart budget and does not unpublish the catalog. A failed
on-demand start publishes the server as unavailable and hands it to the ordinary
bounded restart supervisor. `idle_stopped` reports the state. The default,
`None`, keeps the child up for the adapter's lifetime.

A child that owns other processes (for example a browser) must stop them on
`SIGTERM`; the adapter terminates only its direct child and kills it after three
seconds.
