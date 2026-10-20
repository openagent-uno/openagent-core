# Selective quarantined legacy-child adoption

`openagent_storage_sqlite.legacy_child_adoption` is an offline migration API for one historical delegation projection. It is not run admission, identity discovery or a grant to read/replay the child. It never starts a deployment or contacts a model/provider.

This initial contract deliberately accepts exactly thirteen original rows across six canonical tables: one session, one run, three messages, two domain events, one activity item and five search-outbox intents. Additional foreign tenants/rows, ambiguous parent links, changed hashes or unknown immutable-history triggers are rejected. It does not generalize this observed shape to arbitrary histories.

## Host provenance and authority

First stop and durably fence every writer/ingress. Use `prepare_migration` with a **new** backup and candidate directory. SQLite backup includes coherent WAL state; never copy an old main database and then overlay WAL files.

`HostAttestation` records opaque workspace, agent and storage identities, the fence ID, storage instance, source/target tenants, verified backup-manifest/SQLite hashes and the original parent execution-context hash. The field `pvc_uid` is an opaque **stable host storage identity**; a Kubernetes PVC UID is one possible value, not a Kubernetes dependency. A cold-file export identity qualifies only that offline copy and must never be described as a live-volume attestation.

The host independently verifies this mapping through `authorize(attestation, parent_link)`. Returning true because the SQLite rows contain those same identifiers is not authorization. The current operator's migration authority is distinct from the historical execution context and does not create a historical owner.

- `snapshot`: the verified backup directory containing `.openagent-state-manifest.json`.
- `database`: its declared relative canonical database path.
- `snapshot_sha256`: SHA-256 of the manifest's exact bytes.
- `database_sha256`: SHA-256 of the consolidated SQLite backup declared in that manifest, not the cold main-file hash before WAL consolidation.
- `parent_context_sha256`: SHA-256 of canonical JSON for the immutable parent run's `metadata_json.execution_context`.
- `ExactRow`: an allowed table, exact primary-key value and version-one full SQL-row hash from `legacy_row_sha256`; these preserve SQL types, BLOBs and exact stored JSON text. Evidence generated with another hash algorithm must be recomputed from the independently verified snapshot, never silently treated as equivalent.

`ParentToolLink` binds canonical parent session/run, canonical legacy child session/run, original provider child-run ID and exact tool-call ID. The validator reads the runtime-owned parent's `raw_envelope_json.tools`: `tool_name=delegate_task_to_member`, top-level `child_session_id`/`child_run_id` and `tool_args.member_id`. It verifies the actual parent context/owner/tenant/agent/audience and the child's original run envelope. ID prefixes, timestamps and text mentioning a child are insufficient.

## API and effects

```python
from openagent_storage_sqlite.legacy_child_adoption import (
    HostAttestation, ExactRow, ParentToolLink,
    plan_legacy_child_adoption, apply_legacy_child_adoption,
)

# Inside the host's migrate(candidate) callback under prepare_migration's fence:
plan = plan_legacy_child_adoption(
    backup_directory, "state.sqlite", attestation=host_attestation,
    link=verified_parent_tool_link, rows=exact_rows,
)
receipt = apply_legacy_child_adoption(
    candidate_directory, plan, authorize=host_authorize_exact_mapping,
)
```

Planning is read-only. Apply accepts only a separate, untouched prepared candidate, with its normal runtime writer lock held and current host authorization. It compares the complete logical database and original schema before/after the transaction. Allowed changes are the thirteen tenant bindings, the child's proven parent/root session links and its `delegated_parent_run_id`; the intra-session `parent_run_id` remains unchanged. A durable additive receipt records original/new row hashes and provenance without storing content or credentials.

Only the two known canonical event/outbox `no_update` triggers are suspended inside that exclusive candidate transaction and recreated byte-for-byte before commit. Foreign keys, tenant checks, privacy guards and every other trigger remain active. Any unexpected side effect rolls back. Primary keys, content, authors, NULL owner, quarantine, ACLs, timestamps, raw legacy data and `runtime_contract` are preserved. No event is replayed or delivery acknowledged. `check_storage_tenant` continues to run unchanged.

The same plan is idempotent on an unchanged adopted candidate. Drift in the snapshot, candidate or receipt is rejected. After activation and legitimate new writes, re-running this offline migration is not a supported workflow; preserve its receipt as historical evidence.

Check that idempotence before opening the candidate through a runtime adapter.
Even passive adapter startup may advance the `search_outbox` AUTOINCREMENT
counter while restoring already-present native search intents. That existing
maintenance does not change history or replay work, but it makes the candidate
logically different: a subsequent offline apply correctly rejects the drift.
Qualify startup separately, comparing exact history/ACL/cohort rows and recording
any precisely bounded maintenance counters rather than treating startup as a
byte-preserving operation.

## Quarantine and reconciliation

Quarantined sessions remain unreadable through `SessionInspection` and the SQLite public session/run readers even when a host authorizer would otherwise allow access. Session list/search and ancestry/child projections omit quarantined records. Run admission also rejects quarantined sessions before native metadata or messages can be written. Moving a tenant never invents an owner or broadens an ACL.

The canonical legacy projector recognizes the audited adoption only when all thirteen adopted row hashes and the exact legacy projection source hash still agree. Reconciliation then preserves the adopted namespace/lineage without converting it into a native run. Changed legacy data or changed adoption bindings fail closed for explicit host review. Normal startup and restart do not recreate the old foreign projection.

Qualification uses synthetic SQLite histories and actual public readers/projectors. It does not authorize applying a plan to a persistent installation; a host must separately qualify the exact copied snapshot, real storage provenance, backups, startup and activation.
