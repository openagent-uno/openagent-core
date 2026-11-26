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
- `mcp.startup_retry` records the source name, attempt number (1 to 3) and
  requested handshake timeout in `seconds`.

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

## Bounded reconnect for configured sources

A configured stdio or streamable HTTP source can fail before it has a
toolkit or a supervisor. After a timeout, connection loss, or transient
HTTP response (408, 500, 502, 503 or 504), the pool keeps that source and
starts up to three fresh handshakes in the background. The initial connection
and the first reconnect have a 30-second handshake deadline. HTTP keeps this
deadline on every retry. A stdio subprocess must initialize from scratch on
each relaunch. When a background retry starts a new handshake after a stdio
timeout, its deadline is 60, 120 or 240 seconds on the first, second or third
retry respectively. Other retryable stdio failures use the ordinary deadline.
Callers joining a supervised reconnect can reach their own wait limit while
the shared handshake continues.
Tool calls retain their own read timeouts. The first retry waits five
seconds by default and the next two wait twice and four times that interval. Set
`OPENAGENT_MCP_STARTUP_RETRY_SECONDS` in the runtime environment to change
the base interval (0.05 to 300 seconds). One initial recovery handshake runs
at a time per pool. During backoff for a stdio timeout, a foreground caller
joins the scheduled recovery within its usual wait limit. Other callers can
join the same recovery. A caller that times out or cancels leaves the retry running.

If all three attempts fail transiently, the pool waits for authorized discovery
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

The model broker's source listing and tool searches for that exact source
then report `status: "temporarily_unavailable"` and zero tools. The model's
capability map shows the same state. `CapabilityCatalog.discover()` returns
only tool descriptors, so a pending source has no entry in inventories built
from it. The pending registration issues no callable reference or schema. To a
caller without status visibility, exact discovery has the same result as an
unknown source. Missing executables, TLS failures and authorization errors
retain their operator diagnostics. An established session whose reconnect fails
can enter the same bounded cycle. Reconnection performs a handshake and reads
the new catalog. The caller must select a current reference before acting.

## Previously discovered references

The model broker can return `{"status": "stale_reference"}` when an exact
reference previously issued to this execution context has a new definition
on the same registration. It can report `temporarily_unavailable` during a
temporary outage if current policy permits both status and the exact tool.
Both outcomes occur before sending a tool call. Fresh discovery must still
pass current source, audience and tool policy. The broker never substitutes
another source with a matching name.

A complete MCP catalog refresh also tracks each continuously present tool.
Removing a tool and restoring it with identical metadata issues a fresh
reference, even on the same connection. Other unchanged tools keep their
references. Model and app calls recheck the selected tool across credential
resolution and transport awaits. The regression in
`tests/public/test_mcp_app_connection_generation.py` also removes and
restores a tool while its pre-call ping is in flight.

Evidence is held in memory for the 4,096 most recently discovered reference
and context pairs. Removal, replacement, revocation, a different audience,
eviction or a process restart leaves the ordinary unknown-reference response.
This preserves privacy when there is insufficient evidence to diagnose an old
reference. The cache stores a fingerprint of the execution context and retains neither its principals nor response payloads.

`tests/public/test_mcp_initial_timeout.py` exercises a real stdio subprocess
and an authenticated agent turn. `tests/public/test_mcp_recovery_boundaries.py`
covers HTTP errors, a refused TCP connection, reconnect, reload and restart.
It also drops the response after a test server completes a write, verifies the
write occurred once, and checks that a later read can use the restored service.
