# Optional MCP Apps host service

`openagent_core.mcp_apps.McpAppService` composes over an explicitly supplied `CapabilityCatalog`. It provides no HTTP server, renderer, product identity, browser, native UI or default permissions. A host resolves a fresh `ExecutionContext` for every operation and binds the optional `mcp_apps` service when it wants current app data in an engine prompt.

A view captures an opaque ToolRef, exact context fingerprint and originating source. Resource reads and sibling app-tool calls resolve that binding under current policy. Closing, revoking or shutting down a view prevents a later authorization continuation from admitting an action. App metadata and display values cannot select another principal, source or executor.

Hosts can provide a trusted `display_reader(context)` at open. `saved_invocation_display` reads the exact authorized stored invocation, verifies its ToolRef, and distinguishes pending, complete, cancelled and failed execution. Authorization polling can deliver an original result that finishes after the view opens. A successful null result is represented as an empty MCP CallToolResult; a durable `CancelledError` produces cancellation. `display_received` stops repeated terminal payload transfer. The callback and invocation selector never come from app HTML.

`ui/update-model-context` stores bounded external data for the current viewer. The engine includes freshly authorized data as `untrusted_app_context`, without rewriting user input or shared history. Deferred execution receives no app prompt context. Context remains data: it grants no identity, permission, audience or framework instruction.

## Memory and delivery bounds

Defaults apply across the service, rather than multiplying the retained payload budget by the number of views:

- 16 MiB of serialized completed-result bytes in an LRU cache.
- 32 active work/delivery cohorts, held through fresh publication authorization and the last pending waiter.
- 8,192 reserved request IDs globally and 2,048 per view.
- 64 views, 2 MiB request payloads and 8 MiB result/opening payloads.
- 64 KiB model context per view, with 256 KiB aggregate prompt data and a three-second authorization budget.

These are payload and concurrency bounds, not a promise that the whole process uses 16 MiB. Current deliveries, host providers and the renderer have separate transient storage.

A completed ledger entry retains a digest, terminal state and bounded error strings. It retains no completed Task, provider exception, traceback or original request body. Current callers and already joined pending duplicates receive their complete serialized result even if another completion evicts its replay cache entry. Disconnecting a duplicate waiter does not cancel the original operation. Result bounds include JSON escaping, matching the HTTP transport budget. Once evicted, a repeated ID returns an explicit expired-outcome error: it cannot execute the operation again. Inspect effects before choosing a new request ID. Cancellation and unknown external effects are never automatically retried.

Close releases cached data and reserved ledger entries and cancels admitted work. Shutdown also drains pending providers, including retired work that has not yet settled. A closed view cannot repopulate its cache through a late completion.

These APIs are included in Core `1.1.0b39`, which also binds pool-backed views and tool references to an exact MCP connection generation. Earlier
qualification used local candidate artifacts; see the [integration release
boundary](releases/v1.1.0-beta.39.md). A product must still compose its own authenticated transport, exact-owner presentation, approvals and sandbox policy; Core does not imply third-party service or signed-release acceptance.
