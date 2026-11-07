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

## Resuming a conversation and recovering omitted details

Durable history, the model's working context and curated Vault notes are distinct
surfaces. Merely storing a transcript does not prove that a provider received it
on its next call. A host must keep the same session identity and storage, preserve
its current authorization, and select the ordinary history-enabled engine path.
Core does not create a fresh chat merely because it has been inactive for days.
Explicit resets, deletion, identity/audience changes and host retention policy
are separate boundaries; history tools do not bypass them.

The engine's in-place compaction is enabled by default. Its ordinary trigger is
75% of the resolved model context or the configured history cost ceiling
(150,000 tokens by default), whichever is smaller. It preserves the latest four
runs verbatim and summarizes the older span. The background end-of-turn pass and
start-of-turn safety check share the per-session lock. These defaults are
configurable through `OPENAGENT_COMPACTION_ENABLED`,
`OPENAGENT_COMPACTION_THRESHOLD`, `OPENAGENT_COMPACTION_MAX_HISTORY_TOKENS` and
`OPENAGENT_COMPACTION_KEEP_RUNS`. A summary is lossy; it is not a claim that every
identifier or decision is still in the model's context.

For runtime-admitted native sessions, the canonical original messages remain
available through the optional Sessions module when the provider's compatibility
conversation buffer is compacted. This is not an archival guarantee for historical
legacy-only sessions whose transcript was already replaced or purged.

The unreleased source candidate lets `sessions_read` omit `session_ref`. Core
then resolves the current session from the trusted execution context, keeping the
same read and publish authorization checks. An explicitly supplied reference is
still parsed exactly; invalid or foreign references never fall back to the current
chat. Existing explicit-reference callers are unchanged. The result is paginated
in chronological order; follow returned cursors rather than inventing one.

Sessions prompt revision 2 directs the model to use current context first, read
the current session when a continuation lacks enough detail, and use a narrow
`sessions_search` plus the exact returned reference for another conversation.
It must check authorized history before claiming it cannot remember or asking
for a repetition, and must clarify genuinely ambiguous references. This is
model guidance, not a guarantee that every model follows it. The module remains
optional; Core does not turn on Vault, unscoped recall or cross-chat access.

`tests/public/test_session_resume_tools.py` covers current-session resolution,
pagination, read/publication denial, foreign/invalid references, a four-day aged
canonical transcript reopened through a new runtime, and a real buffer compaction
with an intentionally lossy **stub** summarizer followed by exact history recovery.
These are deterministic storage/tool tests, not real-model or live-channel QA.


## Optional summary latency

`SessionSummaryManager.acreate_session_summary` has a ten-second default
`async_timeout_seconds` budget for the provider call, including retries. On expiry
it cancels and awaits the call and preserves the previous summary. It starts no
background task that could mutate a persisted turn later. Hosts can configure a
positive finite budget on their manager, or explicitly set `None` to opt out.
Caller cancellation propagates. Synchronous summary generation continues to use
the model's own transport timeout. This optional maintenance budget does not
truncate the user's model response or its tool execution.

Runtime adapter failures retain the provider error category even when the
runtime has converted the exception into a terminal event. Only stable error
codes and constant public messages should be displayed to end users; provider
diagnostics remain private.
