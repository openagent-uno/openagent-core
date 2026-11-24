# MCP transport recovery

The engine's MCP pool distinguishes an intentional shutdown from cancellation
caused by a failed streamable-HTTP transport worker. It unwinds the dead session
on its owning supervisor task and starts a new handshake. Concurrent callers
join that reconnect; normal shutdown cancellation is still respected. A failure
during the replacement handshake must also unwind its transport, rather than
loop inside a cancelled AnyIO scope.

If a pre-call ping discovers a closed transport, the tool request has not been
sent and may be sent once on the replacement session. An operation already sent
is not automatically replayed by this recovery path. Capability-generation
checks continue to apply; a caller holding an obsolete generation must resolve
the capability again. Transport delivery failures are classified as transient
for the host's normal retry policy, not as proof an operation had no effect.

## Read deadlines

`OPENAGENT_MCP_READ_TIMEOUT_SECONDS` sets an optional read deadline in seconds.
`OPENAGENT_MCP_READ_TIMEOUT_<NAME>` takes precedence for a server: uppercase its
name and replace punctuation with underscores (`host-a` becomes `HOST_A`). Only
positive finite values are accepted, rounded up to whole seconds. Invalid
server overrides fall back to the global value. With neither set, existing SDK
defaults are unchanged. Raising the read deadline does not raise the HTTP
connection-establishment timeout beyond 30 seconds.

## Diagnostics and qualification

- `mcp.session_transport_lost`: a transport cancelled its supervisor.
- `mcp.session_reconnected`: automatic recovery completed.
- `mcp.session_reconnect_failed`: the replacement handshake failed.
- `mcp.session_reconnect_skipped`: a reconnect was refused or already completed,
  with a `why` field; an unexpectedly exited registered supervisor is replaced.

`tests/public/test_mcp_transport_crash_reconnect.py` exercises a real local
FastMCP HTTP server with injected 502s, outages, handshake failure and slow
reads, plus cancellation and deadline configuration tests. Run it with
`test_mcp_dead_session_reconnect.py`; include `tests/public` on `PYTHONPATH` for
their shared fixtures. Broader source qualification also covers partial-entry,
startup-diagnostics and connection-generation regressions. This is source
qualification, not a published wheel or consumer deployment.

The [7 November source qualification](qualification/mcp-transport-recovery-2026-11-07.json)
records 61 passing targeted cases. The full public run has seven catalogue cases
that also fail on the unmodified baseline in the same environment; it is not an
entirely green suite and does not certify installed packages.

## First stdio handshake

An enabled local stdio source can time out before it has a toolkit or a
supervisor to reconnect. The pool keeps that exact configured source and
starts up to three fresh handshakes in the background. Each uses the normal
connection deadline. The first retry waits five seconds by default and the
next two wait twice and four times that interval. Set
`OPENAGENT_MCP_STARTUP_RETRY_SECONDS` in the runtime environment to change
the base interval (0.05 to 300 seconds). One initial recovery handshake runs
at a time per pool. Parallel turns share the attempt.

If all three attempts time out, the pool waits for authorized source discovery
before opening a new burst. The cooldown is the greater of 60 seconds and
eight times the base interval. Closing the pool or reloading its configuration
cancels pending retries. Once the handshake succeeds, a newly admitted turn
can discover and call the tools. Recovery itself sends no tool call.

A host may grant `tool.discover_source_status` for the exact
`ResourceRef("capability", tenant_id, source_id)` and the entire run audience.
The catalog also requires ordinary `tool.discover` authorization, an active
registration and the current turn's source policy. A source wrapper that
filters its inventory must explicitly support this synchronous predicate to
publish status. The predicate returns a boolean and is called after both
decisions. Hosts using tool-level grants can withhold status when no eligible
descriptor or source-wide grant proves that the audience may know the name.

Authorized source listing and exact tool searches then report
`status: "temporarily_unavailable"` and zero tools. The model's capability
map shows the same state. The pending registration issues no callable
reference or schema. To a caller without status visibility, exact discovery
has the same result as an unknown source. Startup configuration failures
such as missing executables and secret resolution errors retain their
operator diagnostics. This recovery applies to local stdio commands.
