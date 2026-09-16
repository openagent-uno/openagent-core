# OpenAgent v1 implementation and acceptance ledger

Updated 2026-09-16. The complete user-approved architecture remains the scope.
The entries below are implementation evidence and outstanding release gates,
not a declaration that the migration is finished.

## Protected source baseline

Original checkouts and live data have not been reset, moved, or cut over. Git
bundles, source SHAs, branch status, tags, and checksums are in the sibling
`.migration/openagent-v1` evidence directory. The standalone server source was
selected at upstream `1842ce6a82f93bdf34783da3b9d10515439fb49e` after fetching;
the original server checkout remains at its previous commit.

The new core and tools repositories use filtered/imported history. The product
monorepo imports component histories without squashing and namespaces historical
tags. GlassPalace changes live in the isolated `codex/openagent-v1` worktree.
The original GlassPalace documentation edits remain untouched.

## Current implementation

| Area | Implemented | Remaining acceptance |
|---|---|---|
| Packaging | Dependency-free minimum, optional engine/providers, six independent core wheels, compiled vault assets | Final coherent rebuild after remaining API changes |
| Lifecycle | Explicit start/close, instance resources, provider ownership, cancellation-safe SQLite commits | Final frozen normal startup and shutdown |
| Identity | Abstract principals, immutable author/authority/audience, live authorization, preserved historical tenant | GlassPalace two-account web/desktop qualification |
| Runs | Durable idempotent acceptance, exact steering/cancel, lost-response reconciliation, unknown effects, delegated ancestry | Final stack delivery/projection qualification |
| Tools | Opaque references, trusted targets/generations, MCP/PTC/workflow dispatcher, semantic vault effects, durable UI cards | Remaining GlassPalace REST surfaces and final image |
| Prompts/vault | Versioned mandatory composition, 170-rule inventory, recall/save/quality/link/Git/dream-child/reminder parity | Real-provider and final GlassPalace UI runs |
| Product | App/CLI/server/bridge monorepo, native identities, app-scoped dashboard/device registrations, automation grants | Signed distribution and complete updater qualification |
| GlassPalace | Public wheel consumer, no-overlay image, CP delivery/projection, provider/model/skills/session/event/webhook APIs | Workflow/import activation, files/device/voice/WS parity, full-stack UI |
| Data | Fenced snapshot/restore API, additive ancestry, exact history migration, operator tenant attestation | Actual coordinated per-agent cutover remains separate from rehearsal |
| Distribution | Frozen macOS server, Electron build, installed CLI and independent dependency manifests | Final signed assets and full transition update chain |
| Replio reference | Installed fixed-catalog consumer with external identities | Repeat against final library snapshot |

## Verified evidence so far

- The original prompt/tool, vault reminder, and vault quality baseline passed
  74 tests against temporary data before implementation.
- Prompt rule/host context tests pass, including stable/dynamic separation and
  preservation under a changed system prompt.
- Vault quality suite passed 65 cases, including Python/TypeScript rules,
  3,000-note scale, real link renames, and Git versioning.
- Clean installed compiled-vault wheel passed real MCP write/read/patch/quality
  tests without runtime npm builds.
- Catalog tests passed through actual MCP stdio, actual PTC socket dispatch,
  exact device generation/revocation, workflow dispatch, and envelope retention.
- Core run tests passed for two runtime instances, multiuser authors, ambiguous
  restart, deadlines, detached observers, audience revocation, and cancellation
  before delayed acceptance. Additional tests cover durable effects, child
  ancestry, delegated operations, instance registries, and background job isolation.
- GlassPalace's deterministic protocol harness passed authenticated HTTP,
  MCP SDK, real Agent/NativeProvider streaming, and shared SQLite storage. Its
  model HTTP peer is a deterministic fixture, not a production provider.
- Product CLI suite passed 92 tests after consolidating native transport;
  Runtime identity/replay/cancel and catalog tests passed separately.
- Tools component, capability-host, installed wheels, and native Rust unit
  suites have individual evidence in the tools repository. Display-dependent
  and Windows-only cases are explicitly not counted as passed on macOS.

Additional integration evidence (each receipt records its own package snapshot):

- The vault parity gate passed **130/130** deterministic checks, including real
  compiled Node MCP operations, successful versus failed semantic saves/recall,
  exact dream-child ancestry and original prompt/quality/reminder behavior.
- The installed device harness passed actual PAKE/certificate/Iroh ingress for
  two accounts and two devices, six shell/editor/filesystem effects, exact
  target selection, revocation, disconnect, and no fallback.
- Electron exercised registration, native transport, local tool consent, real
  temporary file write/read, target cards, terminal rendering and reload. The
  stricter repeat verified exactly one user/assistant message and no repeated
  execution after reconnect. Its model HTTP peer is deterministic.
- The installed CLI passed real PTY, PAKE and Iroh connection, a fixed catalog,
  response and clean `/exit` using temporary user state.
- A Linux/amd64 GlassPalace image was built offline from hashed wheels; **18/18**
  installed-image HTTP/SSE/MCP/SQLite scenarios passed. This is an intermediate
  image, not the final API-complete build.
- Frozen macOS arm64 server qualification passed help/version/selfcheck, actual
  Iroh gateway, bundled Node with empty PATH, vault write/read/failed patch,
  Python MCP discovery, asset hash verification and clean shutdown.
- A protected copy of real local state passed **885/885 exact session
  comparisons** and retained every original value in **32 legacy tables**.
  The fenced rehearsal included **584 notes and 596 protected files**, including
  identity keys and consents, and verified coherent restore without replaying
  effects. A subsequent read-only comparison confirmed the original database
  remained unchanged. This is a copied-data rehearsal, not a live cutover.
- Core snapshot 8 passed 80 of 81 installed public tests initially; the sole
  failure was the old expected message count after adding the durable tool card.
  The corrected reconstruction test passed, including the exact card identity.
  A final complete run remains required after the remaining source changes.

These checks were run at successive implementation snapshots. All affected suites
must be rerun against final package artifacts after integration stabilizes.

## Required final sequence

1. Complete source ownership and remove all active copies and product imports
   from core. Audit every tool, automation, provider, and gateway entrypoint.
2. Build all packages from clean build directories, install outside the repos,
   record versions/SHA256/digests, and run import/isolation/contract tests.
3. Build the standalone product and GlassPalace worker from those exact packages.
   No downloaded standalone source, overlay, installed-package edits, or private
   core imports are permitted in the GlassPalace build.
4. Rehearse migration on coherent copies of databases, WAL, vault, dashboard,
   configuration, identity keys, and provider state. Verify IDs, authors, ACLs,
   model pins, automation digests, timezone/DST, occurrences, and event dedupe.
5. Exercise authenticated app, CLI, server, GlassPalace web/desktop, two accounts,
   exact Computers, app-only dashboards, simultaneous channels, revocation,
   disconnect/reconnect, run retries, exact stop, sub-agents, and vault/dream mode.
   Record deterministic versus real-provider evidence separately.
6. Produce platform artifacts, verify signatures, and prove the full updater
   transition chain before retiring development in original repositories.
7. For each actual agent cutover: stop ingress, drain/stop exact run, stop old
   writer, coherent backup, migrate+verify, start one new writer, reconcile
   deliveries/cursors, reopen ingress. A rollback after new writes requires a
   verified inverse migration or coordinated restore; an old image alone is
   insufficient and external effects must not be repeated.
8. Final report: exact commits, package versions, digests, test evidence,
   qualified platforms, release/update results, and any unqualified boundaries.

No production rollout, live data migration, real-provider UI qualification, or
signed release/update-chain qualification has been claimed at this checkpoint.
