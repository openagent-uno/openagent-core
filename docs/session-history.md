# Public history after interrupted execution

`SessionInspection.transcript` is the shared history reader used by embedding
hosts and the standalone server. Session authorization is checked before access
and again before publication. Reading history never executes or recovers a run.

The runtime's admitted `session_runs`, message anchors and event journal are
durable independently of the provider's legacy session mirror. If execution
ends before that mirror is saved, public history includes the exact admitted
input, authored stream segments and completed or interrupted tool records.
Canonical additions require the current tenant, authority namespace, agent and
session. An exact matching provider run remains the detailed presentation;
unrelated prompts are never matched by their text. Both sources share the
existing bounded run limit, chronological ordering and canonical run IDs.

Cancellation does not erase an already recorded user, partial answer or tool.
Interrupted/cancelled output diagnostics are not assistant answers. A repeated
terminal stream frame does not duplicate earlier deltas. File references still
pass through the normal fresh session/CAS validation; local paths do not become
public file capabilities.

Tool journal events bind their internal invocation ID to the exact provider
call ID persisted in the binding. Provider status frames update that same
tool slot; a delayed started/empty status cannot erase the canonical completed
result or error. Relevant journal events are consumed incrementally in cursor
order, without loading a second full token/event list or silently truncating it.

`SessionInspection.ancestries` follows persisted parent/root links, regardless
of a display origin label such as `chat`. It rejects cycles, inconsistent roots,
foreign tenants, deleted or quarantined links. It does not infer a parent from
a title, prompt, session-ID prefix or host-supplied display metadata.

Deterministic regressions in `tests/public/test_session_history_restart.py`
cover reopening a real SQLite runtime store, cancellation, exact mirror
deduplication, ordering/limits, authorship, CAS revocation, fresh authorization,
cross-agent/authority isolation and ordinary `Runtime.spawn` ancestry. Published
Core29 reproduces the three original gaps; source tests verify the correction.
This evidence does not assert loss of an embedding host's data and does not
qualify a new published wheel, installer, live provider or production migration.
