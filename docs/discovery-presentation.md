# Discovery presentation API v1

This contract is packaged by the `1.1.0b44` Core and SQLite pair. The final beta
version succeeds the earlier development candidate; published wheels must be
built from the corresponding committed source, never relabeled in place.
Package publication and host adoption are separate gates.
It adds presentation to the existing authorized catalog; it does not install
sources, change authorization or select a different executor.

Trusted host composition configures a pool before connecting it:

```python
from openagent_core import DiscoveryPresentation
from openagent_core.engine import MCPPool

pool = MCPPool.from_config([{"builtin": "tool-search"}], include_defaults=False)
pool.configure_discovery(DiscoveryPresentation(
    capability_sources=frozenset({"records"}),
    capability_limit=50,
    source_summaries={"records": "Records and panels available to this run."},
    tool_descriptions={
        "tool_search_list_servers": "List authorized sources and compact hints. Open list_tools for the full inventory.",
        "tool_search_list_tools": "Search an exact source and follow its pages before deciding an operation is absent.",
    },
))
await pool.connect_all()
pool.bind_capability_catalog(catalog)  # supplied by the trusted runtime host
```

`DISCOVERY_PRESENTATION_API_VERSION` is `1`. `configure_discovery` returns the
same pool and rejects changes while it is connected. The immutable configuration
copies the supplied collections, so changing a host dictionary cannot mutate a
running instance. Closing and reconnecting permits a new presentation lifecycle.
No adapter module, function entrypoint or private catalog API needs replacement.

For each selected source, `tool_search_list_servers` adds `capabilities`, a
deterministically sorted list of authorized tool names and compact descriptions
up to160 characters each. `capabilities_total` retains the complete authorized
count and `capabilities_has_more` states whether the preview was shortened.
The configured limit is1–50, default50. Sources not selected keep the original
response shape; with the default configuration all responses keep that shape.

`source_summaries` may add a `description` only to a source already present in
the authorized response. These static host hints must contain no scoped private
data. Tool hints come exclusively from the same current authorized descriptors
used for counting and discovery. Configuring a missing, denied or revoked source
does not make it appear. Hints and descriptions are data, never access grants.

`tool_descriptions` accepts only the discovery function names.
It changes description metadata after their normal schemas are prepared; input
schemas and callable entrypoints stay intact. Values are nonempty strings with
bounds of2048 characters per source summary and8192 per function description.

The preview is not a full inventory. `tool_search_list_tools` keeps its existing
query, page-size, offset, total and `has_more` contract, and still supplies opaque
references. Describe and call that exact reference in the same authorized run.
Replacement, revocation, another runtime or a missing execution context retain
their existing denial behavior.

## Opt-in named calls

In the source candidate, hosts with a small set of known tools can expose
`tool_search_call_named_tool(source_ref, tool_name, args)` by passing
`named_tools={('records', 'records_read')}` to `DiscoveryPresentation`.
Supply at most 50 exact pairs of catalog source IDs and registered tool names.
With the default empty set, the broker exposes the original four functions.

Named calls resolve only the requested pair in the current authorized catalog
and then invoke its opaque binding through the usual catalog call path. Session
authorization, child tool-family limits, revocations and durable invocation
records apply. Result handling uses the same MCP envelope and media projection
as an opaque-reference call. Tools with an unknown schema can still be inspected
with regular discovery. A missing pair or unavailable binding fails without
substituting another tool or source.

The [targeted qualification](evidence/discovery-presentation-v1-2026-11-02.json)
uses a real built-in discovery toolkit, neutral capability sources and54 tools.
It checks summary bounds, denial and instance isolation, exact references and
dispatch, revocation, schema equality and complete pagination. It does not claim
model/provider, installer, signed bundle or live host acceptance.
