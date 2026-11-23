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
