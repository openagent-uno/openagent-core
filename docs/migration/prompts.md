# Prompt and vault migration contract

The v1 composer is called by `Agent._combined_system_prompt`, which is shared by
normal and streamed turns and by child, workflow, scheduled and event execution.
Provider URLs, model families and the legacy lean execution profile no longer
select a reduced framework or suppress vault reminders and retrieval hooks.

`prompts/rules.json` is the canonical packaged source of mandatory framework and
module instructions. The original v0.21.8 framework and lean prompt are retained
as test fixtures. `prompt-rule-inventory.json` accounts for every source paragraph
with an immutable ID, source revision/line/hash, behavior, destination and test.
Its coverage test compares the entire normalized baseline with the inventory;
vault discipline, storage and quality sections also have verbatim parity tests.

`product-prompt-blocks.json` is a migration export for the OpenAgent product. The
product copies these into its own package: persona, delegation preference,
proactivity, network, identity management and autonomous-behavior defaults are
host contributions. Dashboard instructions accompany only the capability
registered by the originating app. The legacy support prompt is preserved for
an explicit product extension; it never replaces the framework for an agent run.
Neither the product export nor the fixtures are loaded by the core at runtime.

The composer always includes its own framework and applicable module blocks.
Trusted host configuration adds `PromptBlock` values, whose identity cannot
collide with framework blocks. The configurable user system prompt is an
additional `host.system` block. Registrations and authorization remain code
boundaries: prompt text cannot grant a capability or elevate an MCP description.

The provider cache contract separates a stable framework/module/host prefix from
the current identity, audience, run, session, effective catalog and injected
capability instructions. `split_prompt` is the common provider boundary. Runner
caches must retain the effective execution context; a stable prefix checksum
alone is not a runner key. Receipts contain IDs, revisions, provenance and hashes,
never prompt text or credentials, and are exposed by `Agent.prompt_receipt` for
the run journal.

The tool protocol uses the exact opaque reference returned by discovery. Source
and leaf names are discovery hints; they are never reconstructed into refs or
used to select an alternate executor. The canonical-manager/no-raw-write rule
applies even when an operation is unavailable. Long shell tasks retain their
background/terminal-notification discipline. Skills and programmatic calls use
the same discovery contract.

Vault reminders retain the first-turn and every-third-turn default (1, 3, 6,
9, ...). The Agent supplies instance-owned `VaultReminderSettings`; direct legacy
callers retain environment compatibility. Read-only and dry-run contexts keep
mandatory consultation and never bypass restricted writes through another store.

The catalog's vault observer uses trusted registration effects (`vault.read`,
`vault.write`, `vault.search`, `vault.inspect`), never a tool's friendly name.
Content recall additionally requires `vault.recall.path` or `vault.recall.paths`.
Only successful results contribute; transport completion, `isError`, nested
application errors and failed saves do not satisfy the write check. Stable call
IDs deduplicate deliveries. `vault_activity_scope` isolates counts for a public
run even when providers open nested recall collectors.

Validation (isolated fixtures; no user data or credentials):

- `python -m unittest discover -s tests/prompts -v` verifies composition, rule
  coverage, actual Agent/provider integration, cache identity, reminder defaults,
  failed-save handling and the real capability observer.
- The legacy `prompt_date`, `prompt_tool_names`, `vault_recall`, `vault_reminder`
  and `vault_gate` categories retain the established operational regressions.

Real-model discipline and product authenticated UI acceptance remain release
gates. Unit prompt parity alone does not prove a model will obey every rule.

Hosts can pass `Agent(host_context_provider=provider)` implementing public
`HostContextProvider.prompt_context(context)`. Prepare bounded live discovery
before execution and return JSON data without credentials. These contributions
are per-turn dynamic context and cannot enter the cached stable prefix.
Discovery signatures are `list_tools(source_ref)` and `describe_tool(tool_ref)`;
source labels are never callable references.
