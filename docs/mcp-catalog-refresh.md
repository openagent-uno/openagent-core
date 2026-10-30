# Complete MCP catalog snapshots and live local refresh

Source implementation, 30 October 2026. These changes are unreleased; they do
not change the published Core b31 or capability-host b1 artifact receipts.

Both the engine's `MCPTools.build_tools()` and the independent capability-host
stdio adapter now consume the complete `tools/list` sequence. Discovery stops
at 100 pages or 10,000 tools, rejects repeated/empty/non-string cursors and
duplicate tool names, and never publishes an incomplete page sequence. Tool
filters apply after collection, so an included tool on a later page is valid.
An explicit engine refresh atomically replaces the MCP-owned functions and
removes deleted definitions while retaining explicitly registered host functions.
Cancellation of engine connect/initialize propagates to the owning supervisor.

The engine-free stdio adapter additionally consumes
`notifications/tools/list_changed`. It coalesces notification bursts and reads
an entire catalog on a separate task, allowing the stdout reader to keep
answering calls and cancel requests. Reads and publications stay bound to the
exact child process; retired processes cannot refresh the current catalog.
The host's existing state-change callback republishes the new inventory. A
failed refresh removes the stale local catalog and marks the provider
unavailable; a later notification from the same process can restore it. A
catalog read is bounded by the configured startup timeout across all pages.
Closing cancels the owned refresh task. An already dispatched call is not
replayed, and no unavailable executor is replaced by another destination.

JSON-RPC response IDs retain their JSON types. A string or boolean cannot
resolve a numeric request. Malformed result/error envelopes fail that request
with `plugin_protocol_error` while leaving the reader available for subsequent
responses. Existing structured content and metadata preservation is retained.
Plugin TOML persistence now escapes line breaks/control characters in command
arguments, environment values and quoted keys without corrupting Unicode.

## Source adoption and release boundary

The independent capability-host remains engine-free and imports only the neutral
tool protocol. Products source-test through its public Python API or their
existing host-tools facade; they must not vendor a Core overlay into a release.
For development, place this repository's
`packages/capability-host/src` on `PYTHONPATH` along with the configured facade
and neutral tool-protocol source paths. The package root pytest configuration
already sets these paths for the qualification command below.

A release needs a newly versioned capability-host artifact (next candidate b2)
and a new Core version, then exact receiving-product dependency pins, verified
manifests, rebuilt sidecars/installers and device acceptance. No release version,
checksum or installed package was changed by this source work. Directly supplied
SDK sessions remain owned by their caller; this change does not install an MCP
notification handler into an externally supplied engine session. Engine catalog
refresh is explicit; automatic list-change handling here applies to the stdio
capability-host adapter.

## Verification

`tests/public/test_mcp_catalog_refresh.py` covers complete pagination, filtering,
atomic removal, invalid/looping/oversized catalogs, old-process rejection,
connect/initialize cancellation, real subprocess hot refresh/failure recovery,
lossless results, malformed response recovery, exact cancellation requests,
TOML round trips, and refreshed discovery through the public `CapabilityHost`.
See [the source qualification receipt](evidence/mcp-catalog-refresh-2026-10-30.json)
for commands, actual outcomes and the separate missing optional audio dependency.
These are local deterministic source tests, not acceptance of a real remote MCP
provider, native computer-control surface, installer or deployed product.
