# Empty legacy-placeholder namespace adoption

`openagent_storage_sqlite.legacy_empty_child_adoption` is a separate, bounded offline API for a historical **empty placeholder**. It is not the thirteen-row verified-delegation API and does not prove a parent run or tool call. Its four-row contract accepts exactly one row each in `sessions_v2`, `activity_items`, `domain_events` and `search_outbox`.

Only four `tenant_id` cells may change. Primary keys, timestamps, metadata, NULL owner, quarantine, ACL versions, canonical NULL `parent_session_id` and self `root_session_id` remain unchanged. No native `runtime_contract`, author, scope, grant, replay or provider execution is created. The original legacy parent hint is recorded as `claimed_parent_session_id`; `parent_link_verified` stays false and `parent_link.parent_session_id` stays NULL.

## Independent host evidence

`EmptyChildHostAttestation` contains `workspace_id`, `agent_id`, opaque `storage_uid`, `fence_id`, `db_instance_id`, backup manifest/SQLite SHA-256, source/target tenants, `parent_session_id` and `parent_row_sha256`. The parent hash covers the **full SQL row**, using `legacy_row_sha256`; it is not an execution-context hash and does not establish historical delegation authority.

The host independently authorizes the exact storage/snapshot/namespace mapping and verifies that this native parent belongs to the selected workspace/agent with a known canonical owner. A matching legacy hint alone supplies no authority. Source tenant must be exactly `installation:<db_instance_id>`. A cold-copy identity qualifies only that offline copy, never a live production volume.

Use `prepare_migration` while the host holds a durable writer/ingress fence. Its backup manifest includes consolidated SQLite/WAL state. `snapshot` is that backup directory; `database` is a declared relative path. `snapshot_sha256` hashes the exact manifest bytes and `database_sha256` hashes its consolidated SQLite file. Both backup and candidate must be new, separate directories. Never overlay WAL files or mutate a published backup.

```python
from openagent_storage_sqlite.legacy_empty_child_adoption import (
    EmptyChildHostAttestation, plan_legacy_empty_child_adoption,
    apply_legacy_empty_child_adoption,
)

plan = plan_legacy_empty_child_adoption(
    backup_directory, "state.sqlite", attestation=independent_host_attestation,
    child_session_id=exact_child_id, rows=four_exact_rows,
)
receipt = apply_legacy_empty_child_adoption(
    candidate_directory, plan, authorize=host_authorize_exact_mapping,
)
```

`rows` is a tuple of four `ExactRow(table, key, sha256)` values. Planning is read-only; application verifies an untouched prepared candidate, acquires its normal writer lock and checks the trusted host callback before work and again before commit. The callback must verify current independent authority and quiescence; it must not simply agree with identifiers read from the database.

## Empty means no hidden history or bindings

The validator requires empty legacy `runs` and a matching, non-malformed legacy projection with zero runs, messages and tools. It checks canonical references across **all tenants**, not merely foreign rows: runs/messages/tools, context snapshots, typed resource/ACL/artifact links and incoming child/parent/root references must be absent outside the exact four rows and retained legacy reconciliation records. Additional foreign rows or tenants are rejected.

Only the two known immutable event/outbox update triggers are temporarily suspended inside the exclusive candidate transaction and restored identically before commit. Foreign keys, quarantine and tenant guards remain active. Full logical inventory and schema comparisons reject any mutation beyond the four tenant cells. The additive audit receipt uses `mode="empty_placeholder"`; existing thirteen-row receipts and their exact contract are unchanged.

Reapplying the same plan is idempotent only before runtime maintenance or legitimate new writes change the candidate. Qualify restart separately: existing search-intent maintenance can advance an AUTOINCREMENT counter without changing history. Never reinterpret a later rejected offline reapply as permission to omit drift checks.

## Reconciliation and access

The canonical projector recognizes only the explicit empty-placeholder receipt and verifies the unchanged four adopted rows, source hash, emptiness and lack of new bindings. Missing/changed evidence fails closed. It does not promote the legacy hint into parent/root lineage or synthesize an execution context. The parent row hash is validated at plan/apply time only; the parent's later legitimate conversations, updates or deletion do not invalidate this unresolved empty child.

Existing public quarantine guards continue to prevent reads, search/list exposure and native run admission. Namespace repair never grants access. Older releases that do not understand this new receipt mode are not qualified to read an adopted candidate; this source change requires a new successor release, without replacing earlier release assets.

The public tests use synthetic fixtures, including the real Team/model-catalog engine path with a scripted provider-response boundary. Qualification of a copied database, installed release artifacts and production activation require separate evidence. This API itself never starts a runtime, contacts a provider or activates a deployment.
