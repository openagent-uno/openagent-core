<!-- Concrete host labels in this historical record are anonymized; original bytes and hashes are retained in the protected operator audit. -->
# Authorized memory retrieval

Every agent turn retains the framework's existing memory discipline. Recall remains a lead to verify against the current note or source; it is never promoted to an instruction or established fact. The host's system prompt does not replace these rules.

`RuntimeServices.memory_access` supplies the host's canonical history search. `CanonicalHistorySearch` adapts the existing operational corpus through an asynchronous `access_for_principal(principal, context)` callback. `HistoryAccess` contains only verified aliases from the host's identity directory. `HistoryAccess.from_principal` grants only the exact abstract principal key; it cannot claim an unresolved historical account or installation owner.

The operational search checks the initiator and every result recipient against current canonical ACLs **before** pagination. The runtime then checks `memory.read` and `memory.publish` for each typed resource before exposing text. Session hits resolve to `ResourceRef('session', tenant, session_id)`; note hits to `vault-note` and a contained relative path; skills to `skill` and their exact name. Automations and dashboards retain their typed definition/run/view targets. Unknown references, foreign tenants and uncontained note paths are rejected.

The same runtime authorization filters semantic, keyword and skill candidates before automatic recall formats its reminder. Without a trusted execution context or the host memory service, no automatic history is exposed. Read-only notes can be recalled with read and publication permissions; recall never asks for write permission. Live revocations take effect at retrieval. Quality metrics count only the authorized results.

The tool response omits corpus-wide document totals, pending counts and index sequence IDs. A revocation between search and publication invalidates that page's continuation rather than disclosing a cursor derived from hidden hits. Failures disclose no retrieved text in logs.

The SQLite runtime writer appends search intents to the existing `search_outbox` in the same transaction as canonical session, message and tool changes. Acceptance retries create no duplicate message or intent. On the first startup with the repair enabled the store records indexed boundaries for discovering omitted intents from older runtime versions. Operational search maintenance advances one bounded keyset page at a time. Each page commits its cursor and intents together, and a short write budget lets an active SQLite writer finish first. The persisted cursor resumes after restart. Completed databases reuse their checkpoint, and the derived search index can be rebuilt from the retained latest intents.

Chat admission and canonical session reads are available while operational history search reports `warming` with incomplete coverage. The search route resumes the same repair when a hosted runtime has no periodic maintenance consumer. The privacy index consumer can still scrub an already committed removal boundary while older intents are being repaired. PostgreSQL session search reads canonical data directly and has no SQLite intent repair.

SQLite startup applies the historical delegation migration once per database. Partial indexes keep interrupted-run recovery proportional to unfinished runs and tool calls. An upgrade builds those indexes once before making the runtime available.

On a 5.83 GB synthetic SQLite 3.54.1 database with 640,000 messages, 80,000 runs and 80,000 tool calls, beta.168 initialized the store in 28.53 s and repeated initialization in 25.46–40.21 s. With the bounded repair, first initialization took 1.14 s including index creation. Repeat initialization took 1.64–1.84 ms. Foreground admission took 6.62 ms while search was warming. The benchmark measured the SQLite runtime on a local workstation.

Verification lives in `tests/public/test_memory_access.py` and `test_runtime_search_outbox.py`: real SQLite private/shared/multi-tenant history, pagination, canonical grant revocation, abstract-principal MCP invocation, read-only recall, unknown historical owners, atomic failure rollback, tool redaction, idempotent retry, startup repair and derived-index reconstruction. These deterministic checks do not constitute authenticated App or embedding-host UI acceptance.

A session that used a document revision carries its source receipts across restarts. A viewer opening that session through the standalone compatibility API must pass the current document and audience checks. The HTTP host installs a request-scoped session projection guard and checks again before publishing the response. MCP App results use the originating run. A text-only conversation fork cannot preserve these receipts and is refused for protected sessions.

The standalone operational full-text index and the older transcript and vector caches do not have an authenticated viewer. They exclude document-dependent sessions. The first protected read invalidates older indexed session entries, and the canonical reader checks already-issued search candidates before returning their snippets. Authorized clients can still open the conversation through the runtime.


### SQLite attachments

A document-dependent tool or model turn persists its revision receipts with the
content-addressed artifact in the same SQLite transaction. The artifact ID can
be read only while the original session and revisions remain authorized,
including when it has no chat link. For historical rows the attachment resolver
also checks session, message and tool links against the source ledger. A
different presentation link to identical bytes preserves these requirements.
The standalone HTTP gateway checks metadata, filename and bytes at delivery.

An embedded host reading these historical attachments through
`ArtifactRepository` can pass its canonical `runtime` explicitly when it
wraps provider or channel authorization. Its actor still needs ordinary
artifact and session access. Repositories without a verifiable runtime withhold
document-dependent bytes.

Durable mirrors can call `runtime.retained_run_projection(run_id, context,
after=cursor, limit=n)`. It verifies run and replay ACLs and returns a record
and an optional cursor page. Runs that consumed document revisions expose only
status, cursors and opaque events to storage outside the live runtime. Use
`get_run`, `final_answer` or `events_page` to show current content under the
viewer's live authorization. An independent run can still publish its ordinary
result to the durable mirror.

For a separate management identity, pass an async context manager as
`SessionInspection(..., runtime=runtime, document_viewer=resolve)`.
`resolve(management_context, session_id)` verifies the current credential and
yields an execution context for that actor, agent and session with its authorized
audience. The inspection revalidates the underlying protected revisions.

The editor can request `notes` with `limit` and the returned `next_cursor` as
`after`. Each page contains path, revision, title, tags, type, UTF-8 byte size
and the recorded edit timestamp in seconds. An unknown source date is null.
`search/files` matches literal case-insensitive path substrings and returns
the filename metadata. Content search continues to use the full-text index.
