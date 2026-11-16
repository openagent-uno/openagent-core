# System prompt

Every run (chat, delegated work, workflows, scheduled tasks and events) gets one
system prompt, built by `Agent._combined_system_prompt` with
`openagent_core.prompts.PromptComposer`. This page describes how it is put
together, the order of its parts, its size budget and how to add to it.

## Structure

The prompt has two halves, split by `<openagent-turn-context>`
(`split_prompt` is the shared provider boundary):

1. **Stable prefix**, cached by providers. It holds, in this order:
   - the always-on **framework** rules (`core.identity`, `core.principles`,
     `core.reporting`) from `prompts/rules.json`;
   - the **module** blocks of the modules the host enabled: rule ids a
     descriptor selects from `rules.json` (`prompt_rule_ids`, or
     `rule_blocks(...)` in the module's `start`) and `PromptBlock`s a module
     package defines itself (for example `module.sessions.history`);
   - the **host** blocks: product defaults, the agent's own persona
     (`host.system`) and the calendar date (`host.calendar`).
2. **Dynamic tail**, never cached as prefix: the turn context JSON (session and
   run ids, execution context, skills index, host context), then the
   `<capabilities>` map rendered by `capability_map.render_for_turn` from the
   turn's authorized catalog, then the `<session-goal>` block when a goal is
   active.

Each composition records receipts (`id`, `revision`, `provenance`, `sha256`)
in the run journal (`run.prompt`); receipts never contain prompt text.

## Order: by purpose, not by module

Framework and module blocks are sorted by `(section, priority, id)`. The
sections, in order (`prompts.SECTION_ORDER`):

| Section | What it holds | Current blocks |
| --- | --- | --- |
| `identity` | What the agent is and how framework and host rules relate | `core.identity` |
| `principles` | Evidence, provenance, authority, act-then-report | `core.principles` |
| `tools` | How discovery and the capability map work; choosing shell, the agent's browser or screen control on a computer; MCP sources | `core.tools`, `core.computers`, `module.mcp` |
| `routing` | Which capability owns which job; managers; federated search; model pinning | `core.managers`, `module.search.federation`, `module.models` |
| `memory` | The vault: where memory lives, read/save rules, quality, end-of-turn check | `module.vault.storage`, `.discipline`, `.quality`, `.checklist` |
| `history` | Sessions, the current conversation, runs | `module.sessions.history` |
| `automation` | Schedules, workflows, inbound events | `module.scheduler`, `module.workflows`, `module.events` |
| `delegation` | Sub-tasks, team leaders, one-level depth | `module.delegation` |
| `files` | Delivering files to the user | `module.attachments` |
| `skills` | Playbooks (the index itself is in the turn context) | none yet |
| `communication` | How to report the outcome | `core.reporting` |

Blocks with no section, or a section this runtime does not know, keep their
input order after all sectioned blocks. Host blocks always come last, in the
order the host supplied them, whatever section they declare. Because the order
does not depend on module start order, the stable prefix (and its cache key)
is the same for the same set of blocks.

## Budget

Framework plus every module block must stay within **18,000 characters**
(about 4.5k tokens at four characters per token). Single blocks stay within
2,400 characters, the memory section within 4,800 and the tools section within
4,800 (3,200 until 1.1.0b110 added `core.computers`). `tests/prompts/test_prompt_lint.py` enforces this. In 1.1.0b100 the
composed framework and modules went from 33,819 to about 14,900 characters.

The prompt carries routing rules and non-negotiables. Detail that matters only
when a tool is used (search syntax, note format, folder conventions,
maintenance steps) belongs in that tool's description, where the model reads
it at the point of use.

## Writing rules

- **Agnostic.** No product, client or channel names, no other agents' tool
  names, no references to internal design documents.
- **True.** Name sources by their current id in backticks (`schedules`, not
  `scheduler`); name tools only if they exist. Do not describe runtime
  behavior that is not shipped. The lint test checks source names against
  `capability_map.SOURCE_SUMMARIES` and module descriptors; the legacy
  `prompt_tool_names` suite checks tool names against the real registrations.
- **Consistent.** One rule in one place. The framework says how tools,
  evidence and authority work; a module block says when to use its source and
  what never to do with it. Host blocks set policy (for example how much to
  delegate) and must not contradict framework rules.
- **Short.** Lead with when to use the capability, then the rules that change
  behavior. Prefer one sentence per rule.

## Adding a block

1. **A framework or module rule shipped by core:** add an entry to
   `packages/runtime/src/openagent_core/prompts/rules.json` with `id`,
   `revision`, `text`, `provenance`, `section` and `priority`, and select it
   from the module descriptor (`prompt_rule_ids`). Only `core.identity`,
   `core.principles` and `core.reporting` are always on.
2. **A block a module package owns:** define
   `PromptBlock(id, revision, text, provenance, section=..., priority=...)` in
   the package and contribute it through `additional_prompt_blocks` or the
   module's `ModuleContribution.prompt_blocks`. Contribute it only when the
   module's `agent_tools` surface is enabled.
3. **A host block:** pass `PromptBlock`s through a `HostPromptProvider` or
   `_host_prompt_blocks`. Host ids cannot start with `core.` or `module.`.
   Per-turn data goes through a `HostContextProvider`, never into a block.

Choose the section by purpose and a priority that places the block among its
neighbours (existing blocks use 0, 10, 20, ...). Bump the block's `revision`
whenever its text changes. Placeholders such as `{{OPENAGENT_VAULT_PATH}}` are
substituted by the agent at composition time.

Then run `tests/prompts` (composition, receipts, cache boundary, verbatim
memory guard, lint and budget) and `python tests/parity/run_legacy.py`
(tool names). Changing the memory wording also means re-anchoring
`tests/fixtures/prompts/framework-v1.1.0b100.txt` in the same reviewed change.
