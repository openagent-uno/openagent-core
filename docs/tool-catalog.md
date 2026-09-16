# MCP and connected-client catalog

`Runtime.capabilities` is the model-facing authority for discovery and invocation.
The model receives four discovery functions only:

- `tool_search_list_servers()` returns authorized source references and target labels.
- `tool_search_list_tools(source_ref)` returns definitions and opaque tool references.
- `tool_search_describe_tool(tool_ref)` resolves one current definition.
- `tool_search_call_tool(tool_ref, args)` invokes that exact instance and generation.

No model argument selects client/server routing. A source name is a durable
logical binding; a tool reference is a short-lived handle and is not persisted
as workflow authorization. Workflow steps call the public `engine.call_tool`
resolver with their persisted source/tool pair. Missing, ambiguous, replaced,
revoked or unauthorized destinations fail; there is no destination fallback.

## Pool composition

`MCPPool.bind_capability_catalog(catalog, trusted_modules=(), user_sources=())`
registers each connected module/tool source. The pool owns the raw leaf executor;
legacy provider accessors expose only the catalog gateway. A pool cannot belong
to two runtime catalogs. Replacement or shutdown revokes its registered handles.
Bindings require an authenticated `Runtime` execution context; no context means
no discovery or call. The host must classify user-owned sources explicitly from
its trusted registry, and the catalog must permit dynamic installation. Fields
inside an MCP import or a mutable database row do not decide ownership.

Vault write semantics are attached only to the resolved official vault module,
when selected by the host. A custom source named `vault`, arbitrary MCP metadata,
or a tool named `write_note` cannot acquire the privileged write effect. The
runtime observer interprets full success/error envelopes before satisfying a
vault-save reminder.

## Connected clients

Authenticated ingress calls `register_interactive_capabilities(catalog, origin,
source_namespace=...)`, then places the returned leases in its trusted
`ExecutionContext`. The namespace comes from the host connection registry.
The adapter binds the exact device, client instance, generation, authorization
epoch and registry identity. Every discovery/call checks the current verified
origin and registry. Other channels and deferred work have no such lease.
Disconnect/revocation must also revoke the registered source IDs; even before
that cleanup, the registry and origin checks deny new calls.

## PTC and envelopes

The Python bridge accepts `call_tool(tool_ref, args)`. Its socket and container
file transports share the same dispatcher, which captures trusted runtime,
authority, run, origin and policy before starting. The child request carries
only its capability reference, arguments and per-execution RPC token. It cannot
supply or replace principal, audience or destination. Each request rechecks the
current catalog, permissions, configured allowlist and nested-call limit.

MCP results retain `content`, `structuredContent`, `isError`, `_meta`, embedded
resources, child-session references and other extension fields. Function-call
hooks remain in the pool's private leaf executor. The public catalog is the
single place for runtime effects and durable invocation events.

## Optional vault packaging

`openagent-modules` owns the compiled vault resources. Run `npm ci` in the
preserved vault TypeScript source and `node scripts/build-module-assets.mjs`
before building that optional wheel. `resolve_builtin_entry("vault")` uses its
installed assets. Runtime resolution never installs dependencies or compiles
code. The host supplies Node.js, vault directory and explicit configuration.

`tests/public/test_mcp_catalog.py` exercises exact same-name destinations,
revocation/replacement, device/channel separation, model schemas, PTC Unix
socket dispatch, workflow dispatch, actual MCP stdio full envelopes, semantic
effect provenance and fixed/user catalog ownership. `tests/modules` exercises
an installed resource wheel from outside the repository, including real vault
write/read/patch quality and guarded local trash. These deterministic tests do
not qualify authenticated product UI, real model providers, container PTC,
signed installers, system-trash helpers or supported-platform updater paths.
