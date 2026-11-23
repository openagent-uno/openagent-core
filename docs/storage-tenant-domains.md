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

This projection handles private, active, owner-only root chats and their terminal text runs. It keeps the authenticated author and tenant, session and message IDs, per-session message sequence, request fingerprint, admission context, event cursors, run output and activity timestamps. Legacy sessions, tombstones, quarantines, shared audiences, attachments, child sessions, active continuations and reservations require additional runtime mappings. A source with these records remains recoverable in the private history stage. Native candidate creation fails atomically. Validate memory, artifact serving, retained private domains, effects and rollback independently before choosing a live target.

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
