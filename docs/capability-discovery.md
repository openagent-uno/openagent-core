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
