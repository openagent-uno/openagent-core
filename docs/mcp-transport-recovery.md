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
