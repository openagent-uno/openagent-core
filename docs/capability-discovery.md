# Capability discovery and the capability map

Every tool except discovery (`tool_search_*`) is deferred, so the model needs
to know what exists before it can search well.

## The `<capabilities>` section

`openagent_core.capability_map.render_for_turn` renders the turn's authorized
catalog (`runtime.capabilities.discover(context)`) as a plain-text section
appended to the system prompt of every run. Each line is one source on one
target: source id, target label (the computer or account it acts on), tool
count, a use-case description and, for sources with at most 12 tools, the tool
names. It is bounded (about 7k characters) and says when sources were left
out. It never names a tool the current principal cannot call.

Descriptions come from the host's `DiscoveryPresentation.source_summaries`,
then the framework defaults in `capability_map.SOURCE_SUMMARIES`, then the
built-in MCP spec description.

## Forgiving search

- `SOURCE_ALIASES` maps older or common names (`scheduler`, `workflow-manager`,
  `memory-search`, `cron`, `terminal`…) to the current source id.
  `tool_search_list_tools(source_ref=<alias>)` resolves it and reports
  `resolved_from`; an unknown source lists the authorized ones.
- Search terms are lightly stemmed ("schedule", "scheduled", "schedules" match)
  and stopwords are ignored.
- A tool's search document includes its source id, the source description and
  its aliases, so a domain carried only by the source name still matches.
- `tool_search_list_servers(query=…)` requires every query word, matched
  against name, target, description and aliases.
- A tool search with no result returns `suggestions` (sources whose description
  matches) and a `hint` to try other words or list a source.

## Compatible endpoints with JSON Schema

An API-based provider using the OpenAI-compatible driver can opt in with
`tool_transport: json_schema` when its template supports strict JSON output
and lacks native tool calls. OpenAgent sends a schema for one action or final
answer, validates the complete response locally, then runs the selected tool
through the same authorization, execution and receipt path. Tool-dependent
requests buffer one decision before publishing output. For a plain request
without tools, the driver keeps the normal streaming response.

The schema protocol preserves standing system instructions when choosing the
next action. For example, a writable memory module may instruct the agent to
save a lasting preference volunteered during conversation. The decision still
passes through the host's live authorization and normal tool receipt.
For a function call, the model puts the exact advertised name in `next.action`
and its inputs in `next.arguments`. For a final reply, `next.answer` holds the
user-facing content, including JSON when requested. The runtime treats this
field as output even when it contains an example of a tool decision.

The asynchronous gateway offers the current principal's unambiguous
`DiscoveryPresentation.hot_tools` with their actual parameter schemas.
Each decision is resolved to the current catalog reference. An absent,
revoked or ambiguous tool has no shortcut. Function schemas support local
JSON pointers. Tool results and prior assistant calls remain distinguishable
in the text-only template. Model quality is specific to the endpoint. Qualify
a write, an owner read after restart, cross-owner denial and recall from a
fresh session before enabling this mode on a service.

To qualify spontaneous memory, use two newly provisioned personal spaces.
In the first, volunteer a recurring preference containing a random value.
Omit any save request, note path or tool name. Check the model's write receipt,
the exact value and the originating session and run on the new revision.
Restart the host, ask about the preference in a new session without its value
or path, and verify the revision stays unchanged. Check that the other owner
cannot read the existing note. In separate fresh conversations, use a one-time
choice, a fact explicitly excluded from memory, and a synthetic credential.
Compare the vault inventory before and after each of these three cases.

A schema-transport model shortcut requires live permission to call its exact tool at request composition. Source discovery and public inventory continue to use their normal grants. Execution rechecks the call grant again.

During an admitted turn the asynchronous native and schema provider loops
recognize consecutive reads of the same live document revision. Two successful
reads prompt the model to use the evidence or choose a different operation.
A third unchanged read leads to one answer-only decision. Each read executes
normally and retains its own receipt. The runtime checks the current revision,
tool permissions and audience before changing the next decision and rechecks
publication permissions before exposing the answer. A new head, intervening
action, failed read, parallel batch or new turn breaks the sequence.

### Restricting the capability sources of one turn

An embedding host can narrow a run to exact catalog source IDs with
`openagent_core.capability_scope`. Install the scope in the public
`AgentExecutor.request_scope` hook. Derive the choice from the authenticated
execution context and the persisted, fingerprinted request. This keeps retries
and resumed execution on the same policy. A trusted policy may narrow the
set further when an underlying grant expires.

```python
@asynccontextmanager
async def request_scope(request, context, runtime):
    sources = await policy.sources_for(request, context)
    with capability_scope(sources):
        yield request
```

`None` inherits the current scope. An empty set produces a text turn with no
model tools. A nonempty set limits discovery and invocation to those exact
sources and retains the discovery broker on compatible providers. An earlier
tool reference cannot be executed through a narrower scope. Nested scopes
intersect and concurrent runs have independent contexts. Source selection
continues to require the host's ordinary user, tenant, operation, and audience
grants. Session history, automatic memory recall, and the grants protecting
them remain independent.
