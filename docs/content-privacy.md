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
attribution conservatively fences customer redaction within the authorized
store; customer-specific processing remains blocked until that scope is resolved.

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
transcripts, compaction, child sessions, Vault, attachments, files, search, run
replay and session journal. A host must add enabled external surfaces and attest writer instrumentation
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

This candidate instruments selected real public/engine/compaction/child/journal
writers and canonical search refresh, and fences unsupported Vault/channel and
snapshot publication. It does not establish coverage of every historical record,
cached context, media, Vault/Git revision or actual product schema. Until every
enabled writer and historical baseline is qualified through published APIs, the
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

This terminal-capture entrypoint alone qualifies only `terminal-public-run`.
The later checkpoints instrument live source/engine/child publication, and its
optional canonical search adapter requires consumed erasure and WAL cleanup.
The normal required-surface set remains gated: cached engine context, Vault/Git,
external indexes, host snapshots and baseline historical copies remain gaps.
It is not yet connected to the standalone connector pipeline or its public wheel
pins. Do not use this narrower receipt as a Shopify privacy-completion claim.

Source qualification uses real canonical run acceptance, tool intent/result,
assistant publication, read/replay and restart paths. It verifies that selected
private content is absent from the canonical file and WAL, unrelated human input
and Gmail result remain, ordinary events remain immutable, missing/active exact
calls are rejected, and exports remain scoped. These tests do not qualify an
actual provider consent or a running product with the legacy engine adapter.

## Canonical writer coverage and remaining integration

| Surface | Existing canonical write/read point | Required completion work |
| --- | --- | --- |
| Live tool results | `CapabilityCatalog.call_tool` → `RuntimeStore.finish_tool` | Candidate atomic source/fragment/intent hooks covered; host provider schemas and public artifact consumers still require qualification |
| Generated stream | `AgentExecutor.execute` → `RuntimeStore.append_event` | Candidate delta/status/done and inherited origin fences covered; external executor/cached stream copies still need inventory |
| Engine transcripts | runner `aupsert_session` → `memory.store.sqlite.SqliteDb.upsert_session` | Selected raw fragments/final fields and legacy/public copies covered; cached context, arbitrary metadata/session state, events/media remain gaps |
| Compaction | `core.compaction._save_runs` and operational `context_snapshots` | Real recap/fold lineage covered; context_snapshots has no Core writer, host-written/historical snapshots require separate adapters |
| Child sessions | `ExecutionContext.child` and `Runtime.spawn` | Real agent mission/final copies covered; attachments blocked, external executors and copied historical contexts remain gaps |
| Session journal | `MemoryDB.append_session_event` | Candidate canonical transaction/fragment sink covered; actual host raw/user replay schemas and historical rows need qualification |
| Vault | `VaultService.write_note/delete_note/restore_to` and `VaultGit` | Track mixed note revisions/spans and originals; redact historical Git blobs/reflogs with preserved unrelated revisions, rebuild derived indexes and fence restore/autocommit |
| Attachments/files | attachment store and host-managed file writers | Bind managed artifact origins and revisions; erase replicas/derivatives, enforce paths, preserve untracked human edits, exclude arbitrary external shell/MCP writes from claimed coverage |
| Search | operational search outbox/extractor/index | Canonical refresh boundary/FTS/WAL qualified; external/Vault indexes and historical generations remain host inventory |
| State snapshots | pre-DDL backup and public migration/restore | Protected cohorts fenced before copying; historical backups require inventory, managed backup lineage/sink remains future work |
| Export delivery | product-authorized privacy delivery path | Protect export staging, validate destination and explicit recipient authority, support exact request/revision deduplication and durable retry without central content persistence |

The protocol/metadata and terminal sink checkpoints intentionally cannot assert
this table complete. Existing untracked installations need an explicit historical
baseline migration or reviewed reconciliation; a new tracker cannot retrospectively
identify unknown old customer-derived text. Provider admission remains gated until
all enabled surfaces are covered and actual product restart/recovery cases pass.

## Live public writer checkpoint

The candidate `ToolResultPrivacyPublisher` service and host-only
`CapabilityCatalog.register(..., result_provenance=...)` callback run **before**
canonical publication, rather than relying on the existing post-persistence
`EffectObserver`. A source requiring attribution cannot execute without an
explicit canonical publisher. Origins must be a bounded immutable tuple bound to
the current tenant; model arguments cannot install the callback. Local tool
publication authorization is rechecked after the callback.

`SqliteToolResultPrivacyPublisher` writes the exact source tool result, message
anchor, replay event, origin registry and revocation fence in one canonical
transaction. Generated public stream/status, assistant text and output writers
inherit those origins and also fence late publication. New public runs inherit
an admitted parent/earlier-session origin snapshot. All tool admissions, including
sources without their own provenance callback, recheck revoked inherited run
context when the publisher is installed. This cannot undo an external effect
already in flight; external capability recipients require their own policy and
privacy adapter. It does not establish complete child-session content coverage.

The engine's real `FunctionCall.execute/aexecute` dispatch now retains the
provider tool-call identity through nested catalog calls in a context scope.
Canonical invocation bindings include `provider_call_id` while keeping their
separate catalog invocation ID. Concurrent/nested dispatch restores scopes and
does not share identity between tasks. A call identity links content; it grants
no capability or provider/store authority. Deferred lazy generator consumption
and tool caches remain separate writer boundaries.

The legacy engine serializer and compaction writer consume those exact
identities/origins in the checkpoints below; already persisted legacy data still
requires baseline reconciliation. No standalone runtime
installs this publisher or callback yet. These live public-ledger tests remain
source qualification and never establish Shopify readiness or released artifact
compatibility.

## Legacy engine field checkpoint

`EngineContentPrivacyWriter` now participates in the real `SqliteDb` session
upsert transaction before the optional projection savepoint. Privacy fences are
propagated by Agent/Team persistence helpers rather than swallowed as best-effort
projection failures. `SqliteEngineContentPrivacy` derives scope solely from the
public run origin registry and exact `provider_call_id` bindings. Serialized
metadata cannot label its own origin, choose a recipient or acquire a tenant.

The candidate records exact field revisions for selected provider tool results
and arguments, their message anchors, later generated assistant fields and known
output/reasoning/media-reference fields. Raw calls for different customers keep
their specific origins; generated summaries inherit the union. Its selective
sink updates legacy `sessions.runs` and the public run compatibility envelope in
one transaction, preserving original human messages and unrelated raw tool data.
Digest mismatch blocks a changed field. The generation fence prevents a cached
old engine object from restoring private values after redaction.

Real source tests use `SqliteDb.upsert_session/get_session`, the public run store
and its normal serializer/projector. They verify both canonical copies, retained
human/Gmail content, exact separate-customer selection, and rollback of attempted
restoration. The fragment checkpoint below also covers mixed customers in one
normalized JSON response. It still excludes arbitrary session state/provider metadata, event/media
 replicas, prior untracked history and search completion. No full
`engine-history` coverage or product privacy completion is asserted.

## Canonical compaction checkpoint

The real `core.compaction.compact` path now asks the registered writer to validate
folded origins before sending history to the summarizer. Save compares the exact
source digest after model IO, then writes recap content, parent revision lineage
and a durable fold receipt in the same SQLite transaction. Concurrent human
changes block replacement; revocation during summarization rolls the recap back.
Recap IDs use unique opaque suffixes, so two folds cannot reuse a second-level
clock identifier. Source/runtime identifiers and timestamps remain available.

The engine sink follows only these trusted fold receipts to redact original
compatibility copies when a run has moved into a tracked recap. Later recaps
inherit prior recap origins. Existing untracked compaction metadata cannot
manufacture a lineage or completion proof. These tests run the actual compact
reader, writer, serializer and sink, stubbing only the external model response.
They cover normal fold erasure, revocation during model IO and a concurrent human
change. Arbitrary context_snapshots, journal/export/index/media copies and unknown
historical folds remain outside this checkpoint's coverage gates.

## External writer admission and publication

Trusted host registration accepts `privacy_fenced=True` or a mapping of exact
tool names to booleans. The default is false; user sources and MCP annotations
cannot grant this policy. A run without protected origins retains normal tool
access. After a protected read, an unattested tool remains discoverable but is
rejected before API/filesystem IO. This includes the separate Node MCP Vault
writer until it has a canonical privacy adapter. Even attested tools recheck the
generation fence and ordinary authorization. A host must mark a writer attested
only after instrumenting and qualifying every content copy that writer owns.

For canonical files/Vault, `ContentPrivacyStore.publish(revision, publish=...)`
executes a synchronous callback under the same SQLite write fence as enqueue.
It requires the exact revision to have been recorded durably before publication.
The writer first persists its recovery intent/pin and uses a shared writer/sink
mutex. The callback cannot await or reenter the ledger. SQLite cannot roll back
external file/Git changes: the persisted intent and prior graph record must
recover a crash or partial failure. Tests exercise concurrent revocation and a
writer failing after changing its real temporary file; recovery metadata remains
targetable after restart. This primitive alone is not Vault/Git qualification.

## Trusted result fragments

`result_provenance` may return either the legacy whole-result tuple of origins or
`ToolResultProvenance(fragments=(ContentFragment(path, origins), ...))`. Paths are
immutable JSON key/index tuples into the normalized provider result. The trusted
schema adapter must provide a complete, disjoint partition: missing fields,
invalid selectors, overlaps or a foreign tenant reject publication. A fragment
with no origins means the subtree has no provider source (for example host-only
envelope metadata). Provider inventory uses a source-bound
`ContentOrigin(..., nonpersonal=True)` retaining app/shop/generation: customer
requests preserve it, while whole-shop requests erase it. An origin with no
subjects and `nonpersonal=False` means unknown customer scope. Customer-specific processing of such an unknown
scope blocks with `subject_scope_unknown` before invoking any sink. Storewide
requests still match unknown origins, and revoked unknown scope remains fenced.

The SQLite publisher stores only classification metadata and exact field digests.
Protected raw fragments retain separate selectors in invocation result/envelope,
tool message text and replay event result. Invocation arguments are source-bound
as their own query. The actual engine serializer reads this trusted partition by
canonical provider call identity and records JSON subtree revisions in legacy
tool/message copies, including JSON serialized as text. Generated summaries and
attested downstream tool results inherit the origin union. They cannot pretend
to preserve a customer merely because their model text omitted its identifier.

Tests exercise the real publisher, serializer and sinks with one provider result
containing two customers and nonpersonal inventory. Erasing the first preserves
the second in every canonical raw copy, and the second can then be erased with
its own selectors. The catalog callback also rejects an unexpected new field
before canonical publication. These APIs do not infer Shopify selectors from
model output: product/provider schema adapters must classify actual provider
responses before any Shopify source is admitted. Search/index/media replicas,
unknown historical data and consumer coverage gates remain separate work.

## Canonical search completion boundary

`SqliteRuntimeContentPrivacySink(..., search_privacy=...)` requires a refresh
adapter whenever the canonical operational search index exists. Without it,
canonical source erasure stays retryable and the revision cannot report complete.
`SqliteOperationalSearchPrivacy(runtime_store, memory_db)` binds the same canonical
database and calls the public Core `refresh_privacy_search` function. It drains
the actual outbox/projector through the committed boundary under the normal index
owner lock, enables FTS secure deletion, merges tombstones and requires WAL
truncation. Unsupported SQLite FTS versions and retained old reader snapshots
block completion. No whole unrelated index, session or human document is deleted.

The real MemoryDB/FTS test first indexes provider data, demonstrates the missing
adapter blocks completion, then retries through the canonical projector. Both
live FTS results and database/WAL bytes no longer contain the targeted raw value;
human and unrelated Gmail results remain. External indexes, Vault indexes and
unknown historical index generations still require their own inventory/adapters.
`context_snapshots` currently has schema but no Core insert/update entrypoint;
host-written snapshots cannot be claimed covered without their writer adapter.

## Canonical session journal

The real `MemoryDB.append_session_event` delegates protected run publication to
the registered public `SessionJournalPrivacyWriter`. Without that service,
protected payloads are refused before insertion; ordinary journal writes retain
their existing behavior. `SqliteSessionJournalContentPrivacy` owns the same SQLite
transaction for journal data and fragment graph writes. Its sink redacts/exports
only the exact attributed subtree and requires WAL truncation. The default ledger
coverage set now includes `session-journal` explicitly.

Typed generated event kinds use the validated run-origin union. Raw tool/command
and human payload kinds require a host-bound schema resolver returning a complete
`ToolResultProvenance` partition; metadata inside the payload cannot confer source
or tenant authority. Human messages written outside a protected run remain
unattributed and preserved. Tests use the real MemoryDB writer/reader and verify
adapter admission, raw partition export/erasure, retained human/other-customer
payloads, and revocation during schema resolution rolling back the whole insert.
Products must qualify their actual journal payload schemas before declaring this
surface covered; the generic generated-kind adapter is insufficient for arbitrary
host replay formats or previously untracked journal rows.

## JSON encoded inside MCP text

`JsonStringBoundary()` is an explicit typed fragment path component, serialized
as `{"decode_json":true}` in SQLite-owned metadata. For example
`('content', 0, 'text', JsonStringBoundary(), 'data', 'customers', 0)` selects one
customer inside MCP text containing JSON. A provider adapter supplies matching
selectors for both that text copy and `structuredContent`. Partition validation
decodes only explicit boundaries, rejects duplicate keys/invalid JSON, and still
requires every inner leaf to be classified. Sinks compare the selected subtree
digest and reserialize the surrounding text after replacement, preserving other
customers and metadata. They retain the existing MCP envelope and tool contracts.

The mixed-customer canonical test uses the real GlassPalace MCP envelope shape
with text plus structured copies, then the engine's own JSON-text serialization.
Both copies retain the second customer after the first request; its later request
also succeeds, and a later whole-shop request removes source-bound inventory in
every canonical copy. Unclassified, ambiguous or malformed encoded provider JSON cannot
be published through a fragment partition.

## Agent child missions and inherited histories

The real public `Runtime.spawn` path retains host-authenticated agent authorship
and the parent run identity. Protected parent origins are inherited before model
IO. Agent-authored child missions are tracked in accepted input and the public
message anchor; original human input remains separate. The engine writer also
tracks exact child input, generated output and its matching mission message,
without requiring the child to call a provider again. Engine child input must
equal the admitted mission. Redacted missions cannot be reconstructed through
`accepted_request`; the API raises `PrivacyFence` instead of replaying private
input. Children carrying attachments are refused until a managed-copy bridge
attests those replicas.

The canonical spawn test persists a real child engine session, erases its mission
and answer in both public/legacy copies, and retains an older human message and
the parent's original human input/authority. Generated final fields in later
runs also inherit protected history even without a new source call. Arbitrary
cached model-context messages, external child executors and unknown historical
session copies still require explicit inventory and qualified adapters.

## Invocation arguments and unsuccessful tools

Protected arguments are recorded in the invocation/envelope/replay intent before
external API IO, in the same canonical begin-tool transaction. A timeout, crash
or rejected result therefore cannot leave an unowned private query. Successful
source results use a root fragment when no finer partition was supplied; this
separates raw provider results from model-generated arguments. Arguments retain
the origins present when the intent was written, while result records retain
their exact provider customer scope. Error payloads and their message/replay
copies also participate in canonical publication and engine serialization.

Tests place one customer's private data in the later query for another customer,
then erase the query in public and engine copies while retaining the second raw
result. Another real failed invocation proves its query and error copies are
targetable even without a successful provider result.

## Canonical database snapshots

The pre-DDL backup now checks privacy admission in a dedicated pinned read
transaction before creating a backup destination. A later protected writer does
not enter that already checked SQLite snapshot. If the cohort already contains
source-bound privacy revisions (including nonpersonal store data), this unmanaged
backup is refused. The public state-migration snapshot and restore entrypoints
also refuse protected copies before creating destination trees. They still require
the host's real quiescence fence for ordinary files and database migration.

Tests use actual SQLite backup/restore paths: an existing protected cohort makes
no copy, a concurrent later protected write is excluded from the pinned read
snapshot, and an older checksummed protected snapshot cannot be restored into an
untracked writer. Existing historical backups without attribution remain an
explicit host inventory/manual reconciliation gap; no existing backup is deleted.
A future managed backup implementation must register lineage, exact snapshots
and erasure/recovery before this fence can be relaxed. A generic coverage string
does not authorize an unmanaged snapshot of protected state.

## Encrypted local exports and delivery receipts

`EncryptedPrivacyExports` is an opt-in SQLite adapter. The host supplies its
cipher and a current authenticated-principal/destination authorization callback.
Canonical export sinks call `export(request, revision, bytes)`. Each encrypted
copy inherits that exact parent revision in the public ledger before publication
under the same revocation fence; later redaction also erases prior export copies.
The spool has its own host-owned database and contains no plaintext payloads.

`seal(request)` makes a bundle ready only after the canonical export job completes.
That is not delivery completion. `download(request_id, actor)` rechecks local
authorization and each origin fence and returns the scoped bundle with a delivery
token. The host calls `acknowledge` only after its authorized transport delivered
the complete bundle. `delivered` supplies the durable completion state to the
product's privacy receipt processor. Staging, missing delivery, revoked access or
an unavailable sink must never be reported as provider-request completion.

The adapter is not installed automatically and does not select recipients, send
email or forward customer content through the central OAuth broker. Source tests
exercise real encrypted SQLite snapshots, restart, current ACL revocation,
deduplication and later erasure through the ledger. Product transport integration,
canonical runtime cache eviction and full cohort coverage still require their
consumer qualification before Shopify can be admitted.

## First provider invocation scope

Trusted registration may also supply
`invocation_provenance(descriptor, arguments, context, call_id) -> tuple[ContentOrigin]`.
The callback resolves the installation/app/shop/grant before external IO, and the
public publisher `begin_tool` writes that scope, argument revisions and durable
intent atomically. Technical pagination queries can retain source-bound
`nonpersonal=True`; unclassifiable customer queries retain unknown scope and block
customer-specific completion. Source labels in model arguments cannot choose the
provider/app/grant. The provider response's attributed scopes must retain the
exact admitted installation binding; a changed group/generation/app fails
publication. Source revocation is rechecked after resolving scope.

The real catalog test observes a recorded first-source intent inside the API
executor, rejects a response bound to a different shop, and confirms revoked
scope causes no subsequent API invocation. Result-only callbacks remain a
compatibility entrypoint; an official provider host must install both callbacks
before claiming first-invocation crash/privacy coverage.

### Cached model context

`SessionControl.invalidate_context(session_id)` is an opt-in host operation for
background privacy work. Native providers drain active calls, pause admission
while cached engine objects are discarded, and retain durable session rows.
Routing providers invalidate their exact session cache. Dispatcher propagation
includes retired providers and reports unsupported adapters or cleanup errors;
it does not substitute best-effort `close_session` or destructive `forget_session`.
The host must install its source fence before draining and only acknowledge a
privacy action after canonical sinks and this operation succeed. Third-party
provider resume state needs its own adapter and cannot be attested by this API.

After fully covered redaction completes, future turns inherit only retained
origins. A child mission from the old protected run and late writes by that run
remain fenced. Canonical engine upserts compare archived runs with the durable,
selectively redacted compatibility envelope before accepting them: a stale
in-memory run cannot restore deleted fields or move the data into another field.
Only content-free serialization defaults are normalized. Changed historical
human revisions cause a conflict instead of deletion. Compaction provenance
retains surviving source revisions and omits erased source fields.
