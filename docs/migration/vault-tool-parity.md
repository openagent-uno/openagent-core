# Tool and vault parity qualification

The migration retains the 74-case baseline: 5 prompt/tool-name contracts,
4 reminder contracts and 65 vault quality contracts (41 gate/service/Git,
9 Python/TypeScript twins, 15 contradiction-candidate checks). The preservation
inventory records the original rule text and each deliberate protocol adaptation.

The final deterministic suite passes **130 checks** on macOS arm64:

| Suite | Passed | Evidence |
| --- | ---: | --- |
| Preserved baseline plus streaming recall attribution | 90 | `parity-legacy-final.json` and `.log` |
| Vault search and incorrect/missing derived index | 18 | `parity-search.json` and `.log` |
| Framework/module prompt inventory, reminder and authorized effects | 15 | `parity-modern-prompts-final.log` |
| Compiled module manifest and actual Node MCP stdio | 2 | `parity-compiled-vault.log` |
| Actual catalog/provider accounting and durable dream child | 5 | `parity-runtime-final.log` |

Logs and their SHA-256 hashes are recorded in the migration evidence directory
under `.migration/openagent-v1/vault-parity-verification.json`. The tests execute
against the current core source with installed optional dependencies; the Node
stdio tests use precompiled `openagent-modules` assets. Source-level twin tests
intentionally use the TypeScript sources so a stale compiled build cannot mask
validation drift. They require an already installed `tsx`; they never install
or build dependencies or write a harness inside a package.

## Preserved behavior

The baseline covers frontmatter parsing and repair, atomic notes and taxonomy,
dates, real links, rename with inbound-link rewriting, duplicate candidates,
doctor idempotence, rebuildable indexes, 3,000-note incremental indexing,
automatic Git commits with provenance, scoped commits, history/diff/restore,
and the mechanical dream maintenance pass. Git restore/reset tests operate only
on new temporary vaults.

The reminder remains enabled by default and fires at turns 1, 3 and 6. The tests
now use explicit instance settings and close their in-memory SQLite connections.
The streaming recall tests exercise `TeamRouterProvider` with deterministic
runtime events and real catalog calls, then inspect persisted outcome counters,
including cancellation exclusion and bounded note-path capture.

Three runtime defects found during this qualification were corrected:

1. Provider events could attribute a read by name alone. Attribution now comes
   from trusted catalog metadata and a successful result. Custom names, failed
   reads/saves and duplicate result delivery cannot forge successful activity.
   The actual compiled vault MCP is tested through `MCPPool` and the catalog,
   including all canonical write tags and single/multiple-note recall tags.
2. The dream tool imported a removed standalone server. It now imports the
   reusable vault prompt and creates an actual runtime child, preserving the
   initiator, authority, agent authorship and ancestry. Manual execution does not
   create scheduled definitions or duplicate execution records. Failure is
   reported as failure; completion is claimed only after the child succeeds.
3. The maintenance prompt named the scheduler's obsolete transport-prefixed
   function. Only that reference changes to the current `list_scheduled_tasks`
   name. The inventory records this adaptation; all other dream text is compared
   byte for byte with the original fixture. Execution still requires the current
   opaque reference returned by discovery.

The prompt/tool fixture inspects the independent shell package's real public
manifest and the core module registrations. Its catalog check also traverses the
authenticated discovery entrypoint and verifies distinct opaque references.

## Reproduction

Use a virtual environment containing the engine, SQLite store, modules, shell
and standalone gateway dependencies. From the core checkout:

```sh
export PYTHONPATH=packages/runtime/src:packages/storage-sqlite/src:packages/modules/src
python tests/parity/run_legacy.py --json parity-legacy.json
python tests/parity/run_legacy.py --modules test_vault_search test_vault_search_wrong_index --json parity-search.json
python -m unittest discover -s tests/prompts -v
python -m unittest discover -s tests/modules -v
python -m unittest discover -s tests/parity -v
```

The legacy harness supplies a new home, workspace, database and empty provider
configuration per case, denies socket connections, closes async SQLite and vault
services, and drains leftover tasks. Source-only registration inspection does
not launch providers or device tools. Fixtures for real MCP stdio use temporary
vaults and explicit subprocess configuration.

These results qualify deterministic behavior, not model judgment, authenticated
GUI interactions, paid-provider behavior, Windows/Linux packaging, signing or an
updater chain. Those remain separate acceptance gates in the overall migration.
