<!-- Concrete host labels in this historical record are anonymized; original bytes and hashes are retained in the protected operator audit. -->
# Authorized memory retrieval

Every agent turn retains the framework's existing memory discipline. Recall remains a lead to verify against the current note or source; it is never promoted to an instruction or established fact. The host's system prompt does not replace these rules.

`RuntimeServices.memory_access` supplies the host's canonical history search. `CanonicalHistorySearch` adapts the existing operational corpus through an asynchronous `access_for_principal(principal, context)` callback. `HistoryAccess` contains only verified aliases from the host's identity directory. `HistoryAccess.from_principal` grants only the exact abstract principal key; it cannot claim an unresolved historical account or installation owner.

The operational search checks the initiator and every result recipient against current canonical ACLs **before** pagination. The runtime then checks `memory.read` and `memory.publish` for each typed resource before exposing text. Session hits resolve to `ResourceRef('session', tenant, session_id)`; note hits to `vault-note` and a contained relative path; skills to `skill` and their exact name. Automations and dashboards retain their typed definition/run/view targets. Unknown references, foreign tenants and uncontained note paths are rejected.

The same runtime authorization filters semantic, keyword and skill candidates before automatic recall formats its reminder. Without a trusted execution context or the host memory service, no automatic history is exposed. Read-only notes can be recalled with read and publication permissions; recall never asks for write permission. Live revocations take effect at retrieval. Quality metrics count only the authorized results.

The tool response omits corpus-wide document totals, pending counts and index sequence IDs. A revocation between search and publication invalidates that page's continuation rather than disclosing a cursor derived from hidden hits. Failures disclose no retrieved text in logs.

The SQLite runtime writer appends search intents to the existing `search_outbox` in the same transaction as canonical session, message and tool changes. Acceptance retries create no duplicate message or intent. Starting a store restores missing intents for public runtime rows from early beta versions, without changing their authors or contents. The existing search consumer rebuilds its derived index from the retained latest intents after index loss.

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
