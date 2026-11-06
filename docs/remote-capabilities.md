# Optional remote capabilities

Core defines contracts, not a device or account topology. A host may run with
zero clients, one or many clients, zero or many human principals, or non-human
principals. None of the adapters below is installed implicitly. The host owns
identity, current authorization, audience, enrollment and transport.

## Remote MCP sources

`openagent_core.remote_mcp.RemoteMcpRegistry(catalog, transport)` accepts a host
transport exposing `catalog(context, selectors)`, `call(context, scope, name,
arguments)` and `read(context, scope, uri)`. Catalog rows contain neutral
`host_id`, `generation`, `server`, `version`, `catalog_digest` and `tools` fields.
`resource_protocol: "mcp-app-resources/1"` is required only for resource reads.
Catalog limits are 256 sources, 512 tools/source, 4096 tools total and 2 MiB.

Call `await registry.synchronize(context)` before discovery. Its returned
snapshot can be used with `registry.discovery_scope(context, topology)` to avoid
repeating the full topology read for each source. Calls and resource reads
always revalidate the exact host, generation, server, catalog and principal /
session / audience before execution and before returning results. An incomplete
catalog retains previously known sources, but cannot prove they remain usable.
An unavailable topology publishes no source. Tool metadata and declared resource
URIs never authorize access or select a different source. Sources are bounded to
4096 registrations; hosts should bound the lifetime of their catalog instances.

Tool references keep the same public identity across model and app surfaces.
The MCP adapter resolves standard guest tool names within the exact originating
source, preserving established discovery and workflow names.

## Optional engine adapters

`ToolInventoryContext` exposes a bounded current tool result through an ephemeral
execution scope. The host selects the source/tool, context key, normalizer,
instructions and list key; Core interprets no domain identities. Deferred work
receives no interactive inventory. `AgentExecutor` accepts optional
`request_scope`, `command_handler` and `require_final_answer` arguments. The
request scope yields the resolved request; commands return a handled answer or
`None`. The original execution loop remains the sole engine implementation.

## Optional media

`CapabilityCatalog.stream_tool(tool_ref, options, context)` streams bounded JSON
packets from one exact optional executor. Core knows no video codec, desktop,
Electron, network protocol or user count. Authorization is checked before start
and before every published packet; catalog replacement, definition mutation and
lease revocation close the iterator. Closing it releases the producer. Sources
requiring canonical result/invocation provenance are denied until they provide a
streaming privacy publisher. A stream cannot substitute another executor.

`MCPPool.register_media_executor(server_name, executor)` is a trusted host API.
An executor provides `metadata(tool_name)` and an async `stream_tool` iterator.
Replacing it invalidates existing references. The host must qualify its transport
before registration. Ordinary MCP sources and no-client runtimes require no
media executor. `SessionInspection.participants` returns saved visible human
authors across the exact authorized session, independently of transcript pages;
authorship is descriptive and grants no membership or access.

Release cohort: `1.1.0b48`. The public contract suite passed 720 tests and
97 subtests (two optional tests skipped). Package publication, host deployment
and real-device acceptance are recorded separately.

Hosts can pass explicit `unavailable_errors` classes to `ToolInventoryContext`
for optional transport failures. Those errors produce unavailable inventory;
other capabilities remain usable. Cancellation and arbitrary failures are not
swallowed, and Core imports no host transport or identity implementation.
