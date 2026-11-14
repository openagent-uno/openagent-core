# MCP startup recovery and connection diagnostics — local candidate

This source candidate follows the shared-command integration base; it does not
change package versions or claim publication, installed-consumer acceptance or
real provider qualification. Keep already built release assets immutable.

Startup dormant recovery now uses the same recursive dead-session classifier
as call-time recovery. The MCP SDK reports closed streams through
`McpError("Connection closed")`, sometimes inside exception groups. The earlier
startup test compared only the outer exception class with three AnyIO class
names, so these SDK failures exhausted in-place retries without the existing
single fresh-process escalation. This is a source-level defect with prepared regressions, not proof
that it caused any particular deployed connector outage. No tool invocation is
replayed, authentication errors do not trigger this escalation, and cancellation
of recovery propagates to the caller.

`MCPPool.connection_status()` is a local read-only snapshot for every configured
spec, including startup failures with no loaded toolkit. Entries contain
`mcp_name`, `state` (`ready`, `dormant`, `unavailable`), `tool_count` and an
optional `failure` with fixed `code` and `phase` values (`secret_unavailable` /
`configure` when a `${secret:...}` reference cannot be resolved, see
[mcp-secret-references.md](mcp-secret-references.md)). The snapshot includes no
exception messages, command arguments, environment, endpoint, headers, stderr,
remote error data or execution references. It never starts, pings, reconnects or
calls a source. `ready` describes the current complete local tool catalog; it
does not prove that provider credentials or a specific resource operation work.
Hosts must authorize access to the snapshot. Existing raw logging and legacy
`list_mcp_tools()` behavior are outside this API's credential-free contract.

Focused regressions are in `tests/public/test_mcp_startup_diagnostics.py`.
The external private qualification handoff records actual commands and outcomes;
until those are executed, source tests are prepared rather than accepted.

This fix is integrated into the local beta42 release family. The refreshed
installed Core wheel passes687 public tests and97 subtests with PYTHONPATH unset
and all runtime imports verified inside the isolated environment. Earlier source
focused21/21 qualification and its sampled-resource receipt remain preserved in
the connector-recovery Root handoff. No provider operation or French acceptance
is implied by either result.
