# Storage tenant checks and legacy projections

This correction is included in the beta31 Core family. Package qualification
and installed-host acceptance remain separate. The tenant check performs no
provider calls and never activates a copied store.

`check_storage_tenant(database, expected_tenant)` reads the existing SQLite
store, including its WAL, in one read-only transaction. A missing or empty
store can use the host-selected tenant. Existing canonical runtime rows must
belong to that tenant; this API never rewrites identifiers or grants access.

The public legacy `SqliteDb`/`MemoryDB` writers and `SqliteRuntimeStore` may
share one physical database. Legacy projections derive their namespace from
the database's `network_id`, or from `installation:<db_instance_id>` when no
network is configured. An embedding runtime uses its authenticated host tenant.
These normal writes previously made the broad all-table tenant check reject a
subsequent startup, even though neither writer changed tenant ownership.

The source correction recognizes this coexistence only when an admitted run
already establishes the selected canonical runtime tenant. Each additional
legacy session must match its retained legacy source through the ordinary
exact projection verifier. Its nested runs, messages and tools must match too;
only associated projection events, activity and search-outbox rows are covered.
For a normal legacy deletion, the default-off `include_deleted` verifier path
rehydrates the retained header and per-record envelopes, recomputes the source
hash, and compares the normalized content. It additionally requires the
processed delete journal, exact deletion event/version/time, deleted activity,
and resource-bound search deletion intents. The normal read APIs still hide
deleted sessions, and verification never makes a tombstone eligible for reuse.
The local installation namespace alone is insufficient evidence. A canonical
runtime row in that namespace, an unbacked or changed projection, an unrelated
derived row, an unknown tenant-bearing table, or another database identity
still fails closed. Other legacy domains are not implicitly admitted.

A legacy-only store still requires its existing host-attested tenant mapping.
`inspect_snapshot_identity` continues to report all opaque physical tenants;
its output is not an authorization to select one, discard another, or bypass
the startup check. Hosts must preserve coherent backups and current identity.

The regression starts with an empty temporary store, writes a normal admitted
runtime request and a normal provider `AgentSession`, closes both writers, and
checks startup again without changing any bytes. Negative fixtures exercise
foreign runtime identity, absent legacy source, changed projected content,
unknown tables, unrelated derivatives, changed database identity and a
legacy-only store. No provider calls or customer data are used by these tests.
The public deletion sequence is covered as well, including forged event time,
version, absent journal/activity/search proof, and altered retained content.

The Core wheel declares `setuptools>=77.0.3` for its existing PEP 639 SPDX
license metadata. This is a build-backend requirement, not a runtime dependency.

The [beta31 public qualification receipt](evidence/core-b31-public-2026-10-23.json)
records all 26 anonymous full-byte asset checks and 250 tests plus 51 subtests
from a fresh public installation, with no skips. A separate clean engine-only
public-wheel prefix also completed a real embedding startup callback against
an isolated coherent copied store while preserving its canonical history and
access metadata. Neither check activates a live store or qualifies a native
product or provider.

## Inventory of a fenced migration source

`openagent_storage_sqlite.preflight` inspects an existing snapshot produced by
`prepare_migration`. Have the host fence every writer when taking the snapshot.
The existing SQLite backup API includes committed WAL data. The vault, its Git
directory and any associated state must come from the same fence. Treat the
complete snapshot and manifest as private. An online database read or a copy
of the main SQLite file alone does not satisfy this input contract.

```sh
python -m openagent_storage_sqlite.preflight /private/verified-snapshot \
  --database state.sqlite3 --vault memories --expected-tenant "$ATTESTED_TENANT"
```

The trusted host supplies the storage tenant. The inspector verifies manifest
hashes, checks that the selected database is declared, opens the sealed SQLite
backup read-only and checks its tenant using the startup ownership rules. The
existing privacy guard rejects snapshots requiring managed provenance. The
operator must keep the snapshot immutable throughout inspection.

The JSON response gives aggregate counts of canonical sessions, runs, messages,
tool invocations, events, search intents and artifacts. It includes tombstoned
and quarantined session counts; these sets can overlap. For databases retaining
legacy sessions, it also reports the legacy and canonical live projection
counts, missing live projections, and pending or failed reconciliation.
Keep the legacy and canonical totals separate because their identifiers can
overlap. Unfinished runtime runs and invalid foreign keys are flagged.

The vault inventory counts regular Markdown files, including derived Markdown.
Its tree digest covers their relative paths and bytes. With Git history enabled
the inspector checks object integrity and reports the count and digest of
commits reachable through refs and reflogs. A shallow, partial or external
object store is rejected. For an intentionally unversioned vault, pass
`--allow-unversioned-vault`. The CLI emits no raw file names, note text,
session identifiers, Git commit messages, tenant identifiers or credentials.

Exit 0 means these source checks found no listed anomalies. Exit 1 includes
aggregate findings to reconcile. Exit 2 rejects the input without emitting
sensitive diagnostics. A successful inventory does not attest destination
parity, file permissions, provider credentials, engine context reconstruction
or a successful restore. Reconcile source and target data under the same fence,
verify scope, history, content, attached bytes and permissions using the
target adapter, and qualify restore before activating the target.

## Transfer and recheck PostgreSQL staging

Install the `postgres-migration` extra of the SQLite adapter. The host supplies a
dedicated private PostgreSQL destination in `OPENAGENT_MIGRATION_POSTGRES_DSN`.
The transfer uses an isolated schema owned exclusively by the migration role.
Its prefix is `oa_stage_` and it is bound permanently to one source
manifest, database, vault and attested tenant. Runtime/API roles must have no
access to this schema or to the migration role. Use encrypted transport and
appropriate backup/retention controls for the sensitive staged history.

```sh
python -m openagent_storage_sqlite.postgres_stage "$SNAPSHOT" \
  --database state.sqlite3 --vault memories --expected-tenant "$ATTESTED_TENANT" \
  --schema oa_stage_initial
python -m openagent_storage_sqlite.postgres_stage "$SNAPSHOT" \
  --database state.sqlite3 --vault memories --expected-tenant "$ATTESTED_TENANT" \
  --schema oa_stage_initial --verify-only
```

The first command inserts bounded parts of the source history and immediately
compares their destination bytes and row counts. It can resume after a crash.
A mismatch stops the copy without overwriting existing data. The second command
only verifies, including unexpected destination parts. Both take the same
destination advisory lock. A result with source issues requires review before
further action.

The staged history includes retained legacy session envelopes and the canonical
session, message, part, run, tool, ACL, artifact/link, event and projection
records. Each table stream begins with a header giving source DDL and column
order; subsequent parts contain newline-delimited JSON arrays. BLOB values are
tagged base64, and real values use tagged IEEE-754 bits. The ordering uses
SQLite row identity or the declared primary key for WITHOUT ROWID tables.
Quarantines, tombstones and references are carried as stored. Each referenced
available artifact requires matching CAS bytes under the artifact root next
to the source database. Unavailable references retain their original state.

This is private migration staging. It does not expose conversations through
runtime APIs, recreate grants, execute source DDL, or replay tools. Vault and
Git history remain in the verified snapshot for their separate import. The
receipt identifies the additional source table count; the host must inventory
those domains separately, together with other databases and private files.
Verify access rules, original public identifiers, history reconstruction and
destination APIs before switching applications.

A source with document-derived session output carries opaque per-run tokens bound to exact vault revisions. The private stage also transfers and verifies the original run bindings, read receipts and revision table when present. A native activation needs the destination to resolve the same tokens under each run's exact tenant, agent, space and audience, including later revocation. Until that projection is qualified, preflight reports `runtime_document_lineage_projection_required` and native candidate creation stops before publication.

### Continuing a verified private conversation on SQLite

A standalone operator can make historical user and assistant messages available to a new model turn after the local writer identity changes. Call `openagent_storage_sqlite.legacy_continuation.attest_private_continuations(snapshot, database, candidate, owner=..., target_agent=..., approved_sessions=..., writer_fence=...)` before enabling the candidate writer. Pass the authenticated human `PrincipalRef`, the current agent ID and a host-verified mapping of each session ID to its exact original agent ID (including an explicitly evidenced null). The source must be a sealed snapshot and the target a distinct copy of the same installation. Keep the candidate offline while running the importer. The returned receipt exposes aggregate counts and the source manifest digest.

Older transcripts can omit internal system and developer entries and number only the retained messages. The importer recognizes this representation only when all retained IDs, original envelopes, authors and positions agree exactly with the filtered source, including tool entries. Internal text stays outside the model window. For an existing deployment, retain the original sealed snapshot and re-run its approved map on the current database under a host writer fence. The additive operation extends the two receipt tables in one transaction, preserves earlier receipts and leaves the conversation tables untouched. Subsequent canonical turns may follow the proven historical transcript. A private rehearsal returns a content-free `receipts_sha256` over the full per-session evidence. Pass that value as `expected_receipts_sha256` with the same approval map and sealed source to make the target independently reconstruct and check the exact receipt set before committing, including on a retry. Release the fence after checking the cumulative counts and confirming that the serving adapter can reopen a newly attested conversation.

The importer matches private ownership, tenant, the original and current ACL, parent provenance and each original message against the canonical transcript. A historical parent retained only in the original session metadata must resolve to a provable private ancestor. A missing parent, conflicting lineage, changed ACL, foreign message author, or unsupported representation excludes affected history. Historical runs with an existing execution context continue to use the native rule. An ambiguous identity stays unassigned. The source and canonical conversation rows retain their IDs, original attribution and content.

At runtime, only a direct personal turn for the same host principal, target agent, installation and exact owner audience can use this evidence. The SQLite adapter checks the current owner, private lineage and message attribution before returning plain user-visible text inside the native history budget. Document-derived runs remain subject to their own revision and audience checks. A retry from the same sealed snapshot checks the stored receipts. PostgreSQL private staging preserves both receipt tables and reports `runtime_private_continuation_projection_required` while the native destination lacks equivalent checks.

### Private PostgreSQL runtime candidate

After staging, the optional native projection creates a fresh, owner-only schema for one agent and the exact sealed set of tenants. Install the SQLite migration extra and the matching PostgreSQL storage adapter. Keep runtime workers disconnected while materializing or verifying the candidate.

```sh
python -m openagent_storage_sqlite.postgres_candidate "$SEALED_SNAPSHOT" \
  --database "$DATABASE_RELATIVE_PATH" --vault "$VAULT_RELATIVE_PATH" \
  --expected-tenant "$ATTESTED_TENANT" --agent-id "$ATTESTED_AGENT" \
  --stage-schema "$HISTORY_STAGE" --candidate-schema "$RUNTIME_CANDIDATE"
```

The candidate name must begin with `oa_candidate_`. Supply the PostgreSQL DSN via `OPENAGENT_MIGRATION_POSTGRES_DSN`. Re-run with `--verify-only` before starting a worker. The operation checks the fenced source and private stage, writes the candidate in a single transaction, compares every projected field and keeps the event cursor high-water mark. A retry verifies a previously committed candidate. Any different row or previous worker admission requires a separate candidate.

This projection handles private, active, owner-only root chats and their terminal text runs. It keeps the authenticated author and tenant, session and message IDs, per-session message sequence, request fingerprint, admission context, event cursors, run output and activity timestamps. Legacy sessions, tombstones, quarantines, shared audiences, child sessions, active continuations and reservations require additional runtime mappings. A source with these records remains recoverable in the private history stage. Native candidate creation fails atomically. Validate memory, artifact serving, retained private domains, effects and rollback independently before choosing a live target.

Completed private tool calls keep their original invocation UUID, call ID, provider alias, arguments, outcome and message anchor. The native reader checks the admitted run audience and session on every lookup. Verification compares the terminal intent with its saved envelope and never executes the tool.

Completed durable operations retain the original segments, immutable delegation, checkpoints and handoff claims. The candidate checks every predecessor, scope and terminal state and writes no new scheduling deadline. Active operations require the host to reconcile the live delegation and scheduler before activation.
### Sealing a history-only source and its tenant inventory

A host whose configured file vault has never existed can declare its exact
relative path while the writer fence is held. The source root must include all
of the host's configured state. Declare every canonical SQLite database, including
any independent grants database. Keep the private manifest and its SHA-256
receipt under host custody outside the snapshot.

```python
prepared = await prepare_migration(
    source, backup, offline_copy, databases=("state.sqlite3",),
    absent_vault="memories", quiesce=writer_fence, migrate=offline_migration,
)
receipt = prepared["manifest_sha256"]
```

This writes a format 2 manifest containing the fenced absence assertion. A
file, an empty directory, a symbolic link or a non-directory ancestor at the
declared path fails the check. Snapshotting, staging and restoring preserve the
absence. Existing format 1 snapshots remain valid for ordinary vault
inventory. They cannot establish that an empty source directory was absent
because their file manifests omit empty directories.

Pass `--history-only --vault memories` to preflight, the private PostgreSQL
stage and the offline candidate; the Python APIs accept `history_only=True`.
Each requires the sealed path and binds the mode to its receipt. SQL-backed
document state still requires its own migration. A host with an unversioned
directory containing files uses `--allow-unversioned-vault`.

For a source with multiple canonical runtime tenants, attest the exact list
for each such database under the same fence. For example, set
`expected_tenants_by_database={"state.sqlite3": [tenant_a, tenant_b]}` in
`prepare_migration`, then repeat `--expected-tenant` for each ID on every
inspection and transfer. Python callers pass `expected_tenants=(tenant_a, tenant_b)`.
The source, the sealed mapping and the caller's set must agree. The manifest
contains private tenant identifiers. Public inventory reports contain
aggregates. Archived rows retain their original tenant IDs. Native projection
also requires supported row types and authorization mapping. A single
`expected_tenant` retains the historical compatibility rules.

Exercise rollback before releasing the writer fence. Restore the sealed source
to a new private directory, then verify it independently while it is offline:

```python
restore_to_new_directory(backup, rollback, expected_manifest_sha256=receipt)
verified = verify_migration_restore(backup, rollback, expected_manifest_sha256=receipt)
```

Verification can be repeated. It checks the pinned manifest, every file's
contents and permissions, the complete file set and any declared vault absence.
Each restore targets a fresh directory. These operations verify state at the
fence. After new writes on a destination, reconcile those writes, cursors,
objects and external-effect receipts under a new fence before switching back.

### Serving preserved input attachments

The private historical stage retains original SQLite records and CAS bytes. To
make supported private input attachments available on the normal authenticated
download URL, use a separate PostgreSQL archive schema and a private object
store. The following example uses an async S3 client supplied by the host. Install the Core engine dependencies, the SQLite adapter’s
`postgres-migration` extra and the host’s async S3 client. The
migration role must own the new schema. Hold the source fence and keep the
archive offline during preparation and independent verification.

```python
from openagent_core.object_artifacts import S3Chunks, SqlObjectArtifacts
from openagent_core.archived_artifacts import SqlArtifactArchive
from openagent_storage_sqlite.artifact_archive import import_artifact_archive

blobs = S3Chunks(async_s3_client, private_bucket, prefix=private_prefix)
settings = dict(
    expected_tenant=tenant, agent_id=agent_id, schema=archive_schema,
    legacy_cas_root=original_cas_directory, dsn=migration_dsn, blobs=blobs,
    history_only=True,
)
receipt = await import_artifact_archive(snapshot, "state.sqlite3", "memories", **settings)
verified = await import_artifact_archive(snapshot, "state.sqlite3", "memories", verify_only=True, **settings)
archive = SqlArtifactArchive(engine, blobs, schema=archive_schema, runtime=lambda: runtime)
publisher = SqlObjectArtifacts(engine, blobs, policy, archive=archive)
```

Use an `oa_archive_` schema name, with up to 40 lowercase letters, digits or
underscores after the prefix. The source must have a clean migration inventory.
The importer compares the original artifact ID, tenant, principal, input
direction, visibility, availability, digest, size, metadata and exact session
links. It binds each admitted attachment to its original run, ordinal, audience
and complete request-reference hash. Historic absolute CAS paths are compared
as strings with the host-attested original directory. Bytes are read only from
the sealed snapshot. Supported shapes are owner-private inputs and private,
active session input links. Other shapes require explicit mappings.

The importer independently reads back all S3 chunks, records their digest and
order, and atomically publishes the PostgreSQL index. Interrupted transfers may
leave private content-addressed chunks; retries conditionally create or verify
them. A verify-only pass reads the source, index and object bytes. A candidate
builder can call `verify_artifact_candidate` from the same migration module,
using its current PostgreSQL connection, immutable SQLite connection, archive
schema, manifest and database digests, agent ID and attested tenants. Only the
returned run IDs qualify for projection with their original attachment request.
Promote the verified read path before accepting new writes. The offline
preparation role checks its private schema privileges until qualification; give
the runtime only read access to the historical index after that check.

Bind the publisher under `attachments.publisher` and pass the host's
authenticated artifact-context resolver to `openagent_gateway.create_app`.
Each historical read checks the live origin sessions, admitted audience, host
policy and protected document receipts, including after each object read.
The owner must still have access to every linked origin. Forgetting a source or
revoking access stops later chunks. The download keeps the original public ID
and any matching session-link ID. Historical input links cannot establish that
a run produced an output. The artifact archive does not activate a runtime.

A host migrating private input attachments first imports and verifies their original links, byte digests and admitted request references in an artifact archive. Pass its explicit schema with `--artifact-archive-schema` when creating and verifying the runtime candidate. The candidate compares the index to the same sealed database and admits attachments only on verified runs. Configure the same archive on the destination reader before activation.

### Host ingress and model media

`openagent_core.artifacts.InputArtifactRepository` describes the host upload,
authorization and bounded read methods shared by the SQLite repository and
`SqlObjectArtifacts`. Before admission, a host resolves the authenticated
product resource and the exact agent/session context, fetches allowed bytes
from its own origin, and calls
`await repository.upload(context, content, filename=name, mime_type=mime, session_id=None)`.
This creates an idempotent owner-private input for that session with no output
link. During an admitted run, pass `session_id=context.session_id` to upload a
session output owned by the current run. A different session ID fails. The
download API supports `read(context, artifact_id=ident)` returning
`ArtifactContent`, and `read_stream(context, ident)` for bounded chunks.

Bind the repository under the typed `openagent_core.artifacts.ModelAttachmentReader`
service. When the publisher also implements this protocol, a single
`attachments.publisher` binding suffices. The model ingestor calls
`model_content(context, reference, max_bytes=remaining)` inside the admitted
execution and supplies the model with typed media carrying verified bytes.
The reader enforces both the object limit and the cumulative
`max_model_input_bytes` budget. Model and publication policy checks continue
throughout the run. A host whose publisher wraps domain-specific output
identifiers can bind the typed reader independently. The public wire reference
contains the artifact ID, MIME, filename, byte count, digest and download URL.
A historical path-bearing request needs its original run, session and exact
archived reference fingerprint. Cloud model ingestion never opens that path.

A host may share a principal across project sessions and deny
`artifact.read` on the principal itself. A verified historical session
link authorizes scoped delivery to its eligible audience. An immutable
attachment on an original model request also has a narrow read grant while
that run, session, exact reference and current authorization all match.
The archive rechecks every source session, document lineage and publish
decision during each read. Direct retrieval outside that model turn uses
the current session link or a separately authorized principal read.

### Reverting the serving build after PostgreSQL accepts writes

A cloud deployment can revert its HTTP and domain behavior while keeping the current
ledger, artifact objects, document revisions and grant authority. Prepare a
standby build of the previous public contract with the same PostgreSQL and
object-store adapters. Pin both image digests. Preserve the host's credential
issuer, grant store, tenant and space mapping, scheduler definitions and policy
semantics. A binary that hardcodes the original SQLite directory cannot serve
this rollback. Its sealed source remains an offline recovery point at cutover.

The optional operator is in `openagent_storage_postgres.serving`. Install the
matching gateway and PostgreSQL adapter, the Core engine dependencies and an
asynchronous S3 client. The deployment controller supplies an authenticated
binding digest covering the sealed candidate receipt, immutable schema and
object namespace, and the identities of the current authorization,
grant, document and archive services. Use stable identities for subscribable
grant and revocation stores so the standby reads the latest decisions.

1. Construct a `PostgresServingGeneration` from each build's actual runtime
   store, immutable deployment ID, expected generation and binding SHA-256.
   Call `await generation.open()`. During initial provisioning, the operator
   calls `await active.install()` once for generation one. An agent without an artifact storage contract passes
   `artifacts=None` on both targets. Its receipt contains an empty object witness
   list. The operator rejects a one-sided binding, an existing artifact index for
   that agent, a migrated artifact archive or earlier object witnesses.
   The private route and change receipts live under the runtime schema and agent ID. A bootstrap
   attempt using an old generation fails after the first change.
2. Call `create_app(..., serving_generation=generation)` for the public gateway.
   Hold `async with generation.serving()` around separate host entry points
   that write, publish or perform external actions. The gateway holds a PostgreSQL
   share lock through the response. Overlapping requests share one lock and
   connection per replica. Reserve a second connection for operators. The
   prospective generation returns HTTP 503 on its public routes until selected.
   Stage the prospective scheduler, dream workers, background jobs and channels
   stopped. Keep its private acceptance probes authenticated and read-only.
3. Provide one `CloudServingTarget(route, artifacts, documents, probe)` per
   deployment. Use the repositories wired to that build. Each `probe()`
   returns `ContractEvidence(allowed_reads_sha256, denied_reads_sha256,
   effect_receipts_sha256)` from independent current reads through its previous
   or current contract. Include data accepted since cutover, event ordering,
   tenant denial, forgotten memory, grant withdrawal, artifact bytes, and
   durable tool/operation receipts. Canonicalize responses before hashing.
   The host asserts the expected authorization and outcomes before returning
   each digest. Keep request bodies, personal IDs, tokens and raw evidence
   in the host's private acceptance environment.
4. Supply an async context manager `writer_fence(source_generation,
   target_generation)`. It drains ingress, external effect dispatch, grant
   writers, vault ingestion, dream and continuation schedulers, device jobs
   and maintenance on both builds and across prefix-sharing replicas. It must
   keep them drained through the database commit and yield the SHA-256 of its
   private attestation. The owner must reconcile ambiguous provider effects
   before attesting. A host can use deployed replica scaling, an existing
   scheduler lease and the grant store's own fence to implement this step.
   If independent writers or roles cannot be fenced, keep the active build.

```python
receipt = await switch_serving_generation(
    active_target, standby_target,
    change_id=deployment_change_uuid, writer_fence=fence, timeout_seconds=60,
)
assert receipt == await standby_target.route.receipt(deployment_change_uuid)
```

The operator takes an exclusive, nonblocking route lock and rejects queued or
running turns, unfinished tool calls, active durable operations and held provider
slots. It challenges both ledger connections and both SQLAlchemy indexes using
fresh PostgreSQL advisory locks, physical relation identity, writable connections
and required role privileges. It checks the
history archive when present and exchanges two private random 32-byte witness
objects through each pair of S3 clients. Configure retention for the
`serving-witness/` prefix according to the deployment audit period. Evidence
mismatch, namespace mismatch or timeout before commit leaves the source
selected. Run an authenticated read on the public destination and direct
traffic and background writers to its selected generation. The old gateway
returns 503. Operator change IDs are private UUIDs. Reuse the same UUID after
an ambiguous response to retrieve the committed receipt. After a subsequent
switch, query `receipt(uuid)` for the historical outcome.

For a return to the newer build, construct it with the next generation and
another change UUID, then repeat the same fenced qualification. Nothing is
copied back or replayed. Every build serves the current PostgreSQL state,
document tombstones, grant decisions and S3 bytes. Database/PITR recovery
and loss of the object-store service have separate recovery procedures.

`tests/public/test_postgres_serving_rollback.py` exercises three independent
HTTP gateway instances, PostgreSQL and S3. It creates durable operations with
non-idempotent tool calls and artifacts for two tenants, withdraws a grant
and forgets a document, switches back, retries an original request, writes a
new operation, and switches forward. It compares event, artifact, document
and effect receipts and exercises stale, in-flight and failed qualifications.
The application-specific previous HTTP contract and production writer fence
must pass their host's own acceptance run before activation.

### Preserve engine session envelopes during a runtime migration

Some SQLite installations contain a legacy `sessions` table alongside canonical sessions and runs. The private historical stage preserves the exact table definition, row order, row identities and bytes, including engine aliases, system prompts, summaries and model metadata. To associate supported envelopes to a runtime candidate, add `--attest-engine-history` to its creation and verification commands, or pass `attest_engine_history=True`. This opt-in is part of the sealed candidate receipt.

Each supported engine run must be a completed single-turn agent exchange. Its session and run identifiers must match one admitted canonical run in a private session belonging to the selected tenant and agent. Input, output, user and assistant messages must agree exactly. Engine agent/user aliases must agree within each envelope. Authority always comes from the canonical admission. Mixed sessions, additional turns, orphaned or duplicate runs and contradictory data stop the transaction. The operator must reconcile histories outside this profile before continuing.

For a history whose canonical admission retained full schema-v1 engine envelopes, pass `--attest-canonical-engine-envelopes` (Python `attest_canonical_engine_envelopes=True`) at creation and verification. This selects a distinct signed-off migration mode. The operator compares every JSON field, including nested tool calls, results and errors, and distinguishes JSON types. Both records must agree on the run and session IDs and terminal outcome. Each canonical row must belong to a live private session under the requested tenant and agent, with its owner recorded as authority, author, initiator and sole audience. A single engine session may contain several turns across engine restarts. The archived session must have an agent alias anchored to at least one admitted envelope, and each envelope must carry the archived user alias. Completed, failed and cancelled outcomes are supported. Missing canonical envelopes, conflicting aliases and ambiguous associations abort the transaction. Creation and verification must select the same mode.

The migration-only `migration_engine_runs` table stores the canonical association, the row and run positions, and SHA-256 digests. The row digest covers the original archived JSON line including its terminating newline. Operator verification rechecks the raw PostgreSQL stage against the sealed SQLite source, regenerates each association and verifies every destination row. Keep both migration tables and the history stage private when granting the application its runtime privileges. The public agent receives its canonical transcript. Older summary and system-prompt contents remain available to the authorized recovery operator in the raw archive.
