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
