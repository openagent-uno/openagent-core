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

On acceptance, the SQLite runtime binds a legacy session whose agent was unset
to the authenticated runtime agent. An exact idempotent replay can repair the
same missing binding after validating the original request fingerprint. A
session already bound to another agent cannot be claimed. Quarantined sessions
continue to require the separate owner-approved adoption flow.

Cancellation does not erase an already recorded user, partial answer or tool.
Interrupted/cancelled output diagnostics are not assistant answers. The model's
own working history keeps a failed or cancelled run as well: its user message
and every tool call whose results were recorded are replayed, an unanswered
trailing tool call is dropped, and the run closes with a fixed note that it
ended without a final answer and that the tool effects did happen. Before this,
a run that failed while generating its final answer was stored without its
executed tools and skipped entirely, so the next turn forgot (and could repeat)
real side effects. Paused runs remain outside history. A repeated
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

## Branching a conversation at a message

`SessionInspection.fork_text(context, session_id, message_id)` creates a
private SQLite session with the visible user and agent text through an exact
persisted message ID. The host must authorize `session.fork` for a direct
authenticated user. Core checks the source tenant, agent and access inside the
write transaction, and builds both the visible transcript and the provider's
history in the same transaction. A fork uses new IDs; it does not copy ACLs,
tools, execution state, summaries, files or attachment grants. It refuses
protected history requiring a provenance-aware copy and prefixes too large to
copy in one transaction.

Canonical `run.child` events can carry `provider_tool_call_id` in addition to
`child_run_id` and `child_session_id`. This optional field comes from the
executing provider call after child admission. A host can correlate it with a
tool ledger binding's `provider_call_id` or a provider status frame. Hosts
should check the viewer's access to the child separately before sending a
link or child stream. Observing the child does not run it again.

Provider tool identities are retained while consuming lazy tool generators.
A live delegation event therefore carries the exact calling tool ID even for a
streamed team tool. Parallel tool generators each keep their own identity.

The unreleased deferred-dispatch fix also passes this identity into the internal
`FunctionCall` used to execute a discovered runtime tool. That call is another
hop of the same invocation, not a new provider call. Without it, the inner
wrapper cleared the identity even though `tool.invoking` had already recorded
it: `run.child` then lacked the key needed to open the still-running child from
its card. Child execution scopes still clear the parent's identity; parallel
delegations do not share it. See the [source qualification](qualification/deferred-child-link-2026-11-12.json).


## Transport-confirmed outgoing messages

`Runtime.record_delivery(ConfirmedDelivery(...), context, source_run_id=...)`
records an already accepted provider message in the recipient's canonical
conversation. The trusted host verifies the provider receipt and maps its exact
account, destination and currently authorized principal before calling Core.
Core rechecks `run.submit` and `run.publish`; an optional source run must be the
current admitted execution in the same runtime. It does not run a model or
invent a user message. A provider/account/conversation/event key is idempotent;
reusing that receipt with different content is refused.

SQLite stores the outgoing text as an independent assistant message, includes
it in paginated history and transcript inspection, and preserves it across
provider history writes and restarts. The next engine prompt receives at most
the latest 20 confirmed messages within 64 KiB, marked as quoted history rather
than instructions. Truncation is explicit; older complete messages remain
available through authorized paginated session history. Native provider
snapshots cannot remove these independently recorded messages.

This generic contract does not grant cross-conversation access, validate a
provider token, or retry an external send. A host must distinguish accepted
delivery from history-recording failure so it cannot accidentally send twice.

## Reset under SQLite contention (beta82)

Team-router session erasure runs its synchronous SQLite store on a worker
thread. This lets the runtime loop finish another connection's pending commit
instead of blocking its own writer for the full SQLite busy timeout. Reset
storage errors propagate through all dispatcher providers: command handlers
report failure rather than acknowledging a reset that did not complete.

The regression holds a real SQLite write transaction whose commit needs the
same event loop, then verifies that forget completes and the stored resume row
is gone. Additional checks cover failed storage and completion of every provider
before a dispatcher error is reported. Long-term memory is outside this operation.

Beta83 also clears persisted resume state before any model has run after
startup. Reset correctness does not depend on a populated provider cache. The
regression starts a dispatcher with no cached model over an existing SQLite
session, then checks that its resume row is erased.
