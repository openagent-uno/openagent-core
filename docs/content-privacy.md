# Product-local derived content privacy

These are opt-in public Core and SQLite contracts. The host authenticates the
provider receipt, checks local access, binds provider/app/store/consent identities,
and owns its durable delivery queue. Core supplies provenance, canonical writers,
local export/redaction and durable per-revision progress. Core exposes no internal
HTTP service and the OAuth broker receives no exported customer content.

## Trusted provenance and admission

`ContentOrigin` binds tenant, source, group and original consent generation.
Generation is independent of a rotating credential version. Customer and order
subjects must come from verified provider schemas. `nonpersonal=True` retains
app/store provenance for inventory and technical metadata: customer redaction
preserves it, while whole-store redaction selects it. Unknown subject scope blocks
customer-specific completion; it does not authorize deleting another customer.

`ContentRevision` identifies an exact canonical artifact, revision and digest.
Parents propagate lineage within the same tenant. Missing or changed parents fail
closed. `ToolResultProvenance` partitions every normalized JSON leaf using typed
`ContentFragment` paths; `JsonStringBoundary` addresses duplicated JSON encoded
inside a text result without deleting unrelated records. Overlaps, unclassified
leaves, duplicate encoded keys and changed digests are rejected.

The host registers `invocation_provenance` and `result_provenance` on
`CapabilityCatalog.register`. Invocation scope is written durably before API IO;
result fragments are validated before canonical publication. These callbacks are
host APIs, not tool arguments or model-controlled annotations. Both local
publication authorization and the exact registration are checked after IO and
again after an asynchronous result callback. A replaced source cannot publish its
private response. Tool failures retain bounded type/effect metadata and do not
silently retry a possible mutation.

A host explicitly binds `ToolResultPrivacyPublisher`,
`EngineContentPrivacyWriter` and `SessionJournalPrivacyWriter` in Runtime services
before recovery and capability admission. A source requiring provenance cannot
execute without the canonical publisher. Protected runs refuse capabilities
without an explicitly qualified canonical writer before IO. This preserves
ordinary custom MCP discovery and ordinary unprotected calls.

## Canonical SQLite services

The ledger and canonical RuntimeStore share one local database. Public adapters:

- `SqliteContentPrivacyStore`: immutable requests, source fences, parent lineage,
  leased jobs, exact revision targets and durable progress.
- `SqliteToolResultPrivacyPublisher`: invocation/result publication, generated
  run/replay provenance, admitted execution origins and source-session resources.
- `SqliteRuntimeContentPrivacySink`: exact public invocation/message/event/output
  export and erasure, retaining run identities and unrelated human/provider data.
- `SqliteEngineContentPrivacy`: canonical legacy/public transcript, recap, child
  mission, rolling summary and changed state-leaf copies. Stale archived runs
  cannot resurrect erased fields. Unscoped restore of an already protected
  session or protected run is fenced; unrelated human sessions are retained.
- `SqliteSessionJournalContentPrivacy`: atomic MemoryDB journal publication and
  selective export/redaction. Generated tool status accepts only the shipped
  bounded `{text: string}` schema; raw tool payloads require a trusted partition.
- `SqliteOperationalSearchPrivacy`: canonical outbox/index/FTS refresh and checked
  erasure. External indexes require their own qualified adapter.

Sinks compare exact revisions and preserve changed human content on conflict.
SQLite cell erasure and a successful truncating WAL checkpoint are required;
blocked readers prevent completion. The ledger snapshots export targets at
request admission and fences matching late redaction writes. Replays are
idempotent only for the same immutable request.

## Pending text delivery and cached context

`StreamSession` binds output frames to origins read from the installed host
publisher. It checks the current source fence before observer/queue publication.
Its public transport methods are `outbound_origins`, `authorize_outbound`,
`release_outbound` and `purge_outbound`. A host enqueues the durable redaction fence
before purging. Matching queued frames are removed; human and unmatched frames
remain. Mixed or unknown customer scope and matching in-flight frames block the
receipt until resolved. A channel revalidates dequeued frames before egress.

Canonical RealtimeChannel rechecks every transport retry and releases pins after
delivery/discard. Its actual sender still receives authorized protected text;
unattested observer/replay-cache callbacks do not receive a second protected copy.
Canonical BatchedChannel checks collected frames and rechecks before returning
its reply. External channel copies require their own delivery/erasure policy.
Protected media and rich attachment output require a managed sink and are fenced.
This text qualification does not attest a media subsystem or arbitrary observers.

`SessionControl.invalidate_context` drains known cached model context while
preserving persistent sessions. The host invokes it after source fencing and
before reporting completion. Unsupported provider resume state or cleanup failure
blocks the request; destructive `forget_session` is not a substitute.

## Coverage and historical baseline

The immutable default required surfaces are transcripts, compaction, children,
Vault, attachments, files, search, run replay and session journal. A host must add
its enabled external persistent surfaces. Missing coverage blocks even a request
with no tracked targets. Configuration or model labels cannot attest coverage.

`SqliteContentPrivacyStore.qualify_empty_history()` admits a new canonical cohort
only after reading the known history/copy tables, configurable legacy session
schemas and the canonical `backups` directory. Nonempty unknown history, orphan
copies, unreadable snapshots and symlinks are retained and block admission. The
baseline result is durable across restart; it is not a retrospective classifier
or permission to delete historical data. Actual writer/fence qualification is
still necessary. A blocked baseline cannot silently become admitted by deleting
its history.

Protected pre-DDL and migration snapshots are fenced before destination creation.
Vault/Git/Node, attachment delivery, unmanaged files, shell/editor writes and
external MCP/channel caches without exact lineage remain unsupported for
protected runs. Registering a filesystem root or relying on a temporal origin
cannot attest arbitrary files or old Git revisions. Managed file/attachment
adapters require trusted exact pins, parent revisions, shared admission locking,
and conflict checks. They do not repair untracked historical copies.

## Encrypted exports and durable completion

`EncryptedPrivacyExports` uses a host-supplied cipher, request authorization and
mandatory per-source revision authorization. Export copies inherit exact parents
in the same source fence and remain encrypted locally. `resource_for_revision`
resolves canonical source sessions; hosts check current `run.read` authority and
bound parent walks for earlier encrypted exports. One inaccessible bundle part
blocks the whole delivery before any plaintext is released.

`seal` means staging is ready, not that the recipient received it. `download`
rechecks access and revocation; `acknowledge` rechecks again before durable delivery
completion. Later erasure also selects earlier encrypted export copies. The host
only marks its provider action complete after all canonical sinks, cache/queue
cleanup and authorized export delivery have succeeded. A broker transport ACK
means durable receipt only.

## Qualification boundary

Source tests use actual Runtime/SqliteDb/MemoryDB writers, catalog invocation,
selective schemas, compaction/children, search, snapshots and encrypted spool APIs.
Consumer tests use owned product routes and text transport with synthetic provider
fixtures. These are distinct from installed wheel qualification and real provider
consent, multi-server credential rotation and privacy-webhook acceptance.

The beta 22 / product beta 39 candidates still require exact installed dependency
closure and release review. Common Shopify enrollment remains disabled until
product-local actions and the real two-server/provider gates pass. No source-unit
receipt alone authorizes activation.
