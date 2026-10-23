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
