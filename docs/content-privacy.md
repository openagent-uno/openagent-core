# Derived content privacy contracts

`openagent_core.content_privacy` provides host-controlled typed provenance and
privacy requests. `openagent_storage_sqlite.content_privacy.SqliteContentPrivacyStore`
provides a durable metadata ledger, transitive inheritance, revocation fences,
worker leases and per-revision completion. Neither installs model tools nor
creates provider credentials. The host authenticates requests, chooses the
recipient and binds provider, store/group, authorization generation and subjects.
Never derive those fields from model arguments or arbitrary tool result labels.

A host records a `ContentRevision` with trusted `ContentOrigin` values before
publishing it. Derived summaries, children and output revisions name their parent
artifact/revision pairs; origins propagate transitively within the same tenant.
Missing parents fail rather than lose attribution. Generation identifies an
original authorization grant, not a rotating token version. A newer verified
grant may coexist with a request limited to older generations. Unknown subject
attribution conservatively matches customer redaction within the authorized
store; it must not turn missing identity into an exemption.

`PrivacyRequest.operation` is `redact` or `export`. Enqueue is idempotent for an
exact immutable request and snapshots matching revisions while admitting the
revocation fence in the same transaction. Matching late writes, including replay
of an old revision, fail with `PrivacyFence`. Other tenants, providers, groups
and later authorizations are preserved. Model-facing gateways must never expose
these trusted registration calls directly.

A worker invokes canonical `ContentPrivacySink` adapters. Each sink applies an
idempotent exact-revision operation, including its replicas and caches, and
returns `complete` only after canonical content is erased/exported or absent.
Conflict with a human edit blocks completion. Exceptions become bounded reason
codes without content or exception messages in receipts. Per-revision markers
survive restarts; leases fence stale workers. A crashed sink operation may repeat
and must be idempotent. The host delivers exports through its authorized,
protected destination; this metadata ledger never archives exported content.

Every ledger retains an immutable required-surface configuration. Defaults cover
transcripts, compaction, child sessions, Vault, attachments, files, search and run
replay. A host must add enabled external surfaces and attest writer instrumentation
and historical baseline coverage before processing. Missing coverage blocks even
requests with no tracked targets. An authenticated broker delivery ACK establishes
durable receipt only; it cannot establish privacy completion.

`ManagedFilePrivacySink` operates on explicitly host-bound files and exact
fingerprints under a managed root. The pin must attest that the whole revision is
provider-derived. All canonical writers share the supplied lock. It rejects path
escapes, symlinks and changed human content. It is not an observer of arbitrary
shell/editor writes and cannot make external MCPs, caches, backups or user files
compliant by registering a root directory. Mixed revisions require their own
span/revision-aware sink, or explicit human review. Local exports must not be
emailed or returned to a provider without the host's explicit delivery policy.

This contract is an implementation checkpoint. It does **not** instrument the
engine's existing transcript, legacy run replay, compaction, Vault or attachment
writers. Until those canonical writers and historical data are covered, the
standalone Shopify enrollment gate remains closed. A successful graph/job test
is not a complete Shopify privacy qualification. Metadata backups and canonical
content backups also need the host's retention/redaction policy.

## Canonical public run ledger checkpoint

`openagent_storage_sqlite.runtime_privacy.SqliteRuntimeContentPrivacySink` captures
**terminal public RuntimeStore records** by trusted, exact provider invocation
call IDs. It selects those invocation envelopes and message anchors, later
assistant/compaction messages in that run, the corresponding durable result
replay events, later generated text/status stream events and final run output. Earlier human input and other raw provider
invocations are preserved. The provenance ledger must share the runtime database;
capture uses the canonical store transaction lane and commits attribution
atomically with its exact snapshots. Active or missing invocations fail closed.

Export reads only those exact revisions through the host-bound callback.
Redaction clears canonical content while retaining session/run identity and
status. The ordinary immutable event trigger is suspended and restored inside
the same exclusive SQLite transaction solely for the selected privacy tombstone;
no other connection observes an unprotected schema. Changed schema/trigger or
changed content blocks the operation. Exact revision markers make retries
idempotent. Started metadata transactions use the public cancellation-drain helper. Managed
file operations have an explicit byte bound. `secure_delete` scrubs replaced cells and a successful truncating
WAL checkpoint is required; a reader holding old pages blocks completion.

Search refresh intents are enqueued with new source versions, but enqueue is not
proof of index redaction. The **only qualified coverage** of this adapter is
`terminal-public-run`; the normal required-surface set remains closed. Legacy
engine projections, active streaming, child-session propagation, Vault/Git,
attachments, context snapshots, search replicas and backups are not qualified.
It is not yet connected to the standalone connector pipeline or its public wheel
pins. Do not use this narrower receipt as a Shopify privacy-completion claim.

Source qualification uses real canonical run acceptance, tool intent/result,
assistant publication, read/replay and restart paths. It verifies that selected
private content is absent from the canonical file and WAL, unrelated human input
and Gmail result remain, ordinary events remain immutable, missing/active exact
calls are rejected, and exports remain scoped. These tests do not qualify an
actual provider consent or a running product with the legacy engine adapter.

## Remaining canonical writer integration

| Surface | Existing canonical write/read point | Required completion work |
| --- | --- | --- |
| Live tool results | `CapabilityCatalog.call_tool` → `RuntimeStore.finish_tool` | Bind origins from trusted host connector executor before publication; atomically commit provenance with content; serialize revocation with in-flight publication |
| Generated stream | `AgentExecutor.execute` → `RuntimeStore.append_event` | Carry run origins through live delta/status/done events and inherited earlier context; fence late writes and cancellation |
| Engine transcripts | runner `aupsert_session` → `memory.store.sqlite.SqliteDb.upsert_session` | Track exact original tool-call/results and derived messages within legacy run envelopes and normalized projections; redact both authorities without broad session deletion |
| Compaction | `core.compaction._save_runs` and operational `context_snapshots` | Carry folded source revisions/predecessors into summary lineage; erase eligible recaps plus legacy copies and reject restoration of revoked generations |
| Child sessions | `ExecutionContext.child` and `Runtime.spawn` | Inherit only the admitted parent snapshot's origins, persist mission/result lineage and cover later child continuation without deleting unrelated human input |
| Vault | `VaultService.write_note/delete_note/restore_to` and `VaultGit` | Track mixed note revisions/spans and originals; redact historical Git blobs/reflogs with preserved unrelated revisions, rebuild derived indexes and fence restore/autocommit |
| Attachments/files | attachment store and host-managed file writers | Bind managed artifact origins and revisions; erase replicas/derivatives, enforce paths, preserve untracked human edits, exclude arbitrary external shell/MCP writes from claimed coverage |
| Search | operational search outbox/extractor/index | Require consumed erasure confirmation and cache/WAL cleanup; an enqueued source-version update alone is not completion |
| Export delivery | product-authorized privacy delivery path | Protect export staging, validate destination and explicit recipient authority, support exact request/revision deduplication and durable retry without central content persistence |

The protocol/metadata and terminal sink checkpoints intentionally cannot assert
this table complete. Existing untracked installations need an explicit historical
baseline migration or reviewed reconciliation; a new tracker cannot retrospectively
identify unknown old customer-derived text. Provider admission remains gated until
all enabled surfaces are covered and actual product restart/recovery cases pass.
