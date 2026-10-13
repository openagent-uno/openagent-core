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
