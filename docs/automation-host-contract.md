<!-- Concrete host labels in this historical record are anonymized; original bytes and hashes are retained in the protected operator audit. -->
# Automation host contract

A host composes `AutomationRuntime` with an execution service; the scheduler never creates an identity or default delegation. Every firing is admitted using `Runtime.execute_operation`, and each model turn uses the same runtime and its canonical child relation. The automation tables are projections and durable input queues; `session_runs` remains the run authority.

## Discovering scheduled tasks in a conversation

`list_scheduled_tasks` returns `results` and `next_cursor`. Each page holds
20 task summaries by default, with a configurable `limit` from 1 to 50.
Pass the returned cursor and the same filters to continue until the cursor is
null. Summaries include schedule metadata and task IDs. Prompts are opt-in.
Use `query` to match a case-insensitive substring of a task name across the
authorized catalog before pagination, including disabled tasks by default. Use `enabled_only`
to restrict the results to active schedules. Use `get_scheduled_task` with a
returned ID to inspect the complete definition, or opt in with
`include_prompt=true` for a narrow list query. The host's read authorization
applies to both tools.

Workflow run history and event delivery history return the same
`results` and `next_cursor` fields. Reuse the cursor with the same
definition and status filters. Pages are ordered by start time with a stable
ID tiebreaker. The shared character budget can shorten a page before its
item limit, so the cursor determines whether more rows are available.

`AutomationRuntime.launch(kind, row, request_id=..., inputs=...)` starts a tracked manual firing without attaching its lifetime to HTTP. `cancel_runs(kind, definition_id, run_ids)` flags only the exact host-authorized projected IDs and preserves the scheduler's cancellation drain. Hosts check each execution's immutable access scope before exposing output or stopping it. Cancellation has been verified on both the workflow projection and canonical run.

## Interrupted execution and projection limits

On startup the scheduler recalculates the next occurrence of enabled tasks.
If a concurrent SQLite write leaves the runtime's shared reader on an older
WAL snapshot, the task update retries on a dedicated writer using the
configured SQLite timeout. The reader's transaction stays open and the
scheduler can complete startup once the update commits.

`SqliteRuntimeStore.recover()` settles unfinished canonical runtime runs as
`interrupted`, with external effects unknown and automatic retry forbidden.
It does not reconcile the separate `task_runs` or `workflow_runs` projections.
The current `AutomationRuntime` worker startup does not invoke the legacy orphan
reapers either. A synthetic restart can therefore leave a projected task row
`running` after its canonical run has become terminal. A dashboard's running
badge, or a durable session remaining active, is not proof of a live execution.

The scheduler records terminal task status before returning, but its projection
write is best effort: a failed write is logged as `task.run_finish_failed`.
A crash cannot run that finalizer. The periodic lease/stale sweep concerns event
deliveries; it does not settle old task projections. Scheduled-task occurrence
claims use the definition's enabled flag and exact next occurrence, not the old
task row's running badge. That badge alone does not prove later schedules are
blocked.

Recovery must retain the exact tenant, definition, occurrence, canonical root and
child relation before assigning a terminal projection result. The public
`RunRecord` exposes canonical run/session/tenant/parent identity, but the current
automation facade has no explicit projection-to-canonical reconciliation port.
Matching display names, timestamps, age, or unverified run IDs is insufficient.
This is an unresolved lifecycle boundary, not permission for a host-side SQL
repair, copied reaper or bulk retry. In particular, the legacy
`requeue_interrupted_task_runs` must not be used to infer that an interrupted
firing had no external effects.

A cancelled, timed-out or interrupted run may already have published externally.
Provider delivery evidence and the run's terminal lifecycle are separate facts.
Reconciliation must preserve both; it must neither claim a confirmed publication
was undone nor retry the occurrence merely to clear a stale projection.

`execution_service.schedule_authorized(workflow, schedule)` is an optional policy check before a scheduled occurrence is advanced or dispatched. Hosts that use distinct trigger owners implement it; otherwise the scheduler uses `definition_authorized("workflow", row)`. The resulting firing must retain that exact trigger reference during credential renewal. Manual occurrence references are separate from the definition grant.

`EventAdministration.enqueue` accepts one or several payloads under one transaction. With `capture_delivery`, the host captures each occurrence's delegation before any queue row becomes visible. Failure rolls back the entire batch and `last_triggered_at`. `capture` remains the fallback for hosts whose current contract captures the definition at manual trigger. Encrypted webhook secrets and definition ownership remain unchanged when the host uses occurrence capture.

`WebhookIngress` verifies original bytes and a fresh host policy, then serializes deduplication, rate limits, timestamp update and unclaimed delivery creation in one transaction. Transport/workload authentication belongs to the host. It creates no session, actor or execution credential. `webhook_auth` imports without model, database or cryptography initialization; encryption is loaded only when authenticating.

`automation_definitions` exposes graph validation, block specifications and timezone-aware schedule calculations/projection. `workflow_schedules.timezone` is an additive nullable column. Migration retains previous IDs and deadlines; synchronizing an unchanged cron and timezone retains the deadline. Changing the timezone recalculates the next occurrence using IANA/DST rules. Historical rows without a zone retain UTC behavior.

Evidence: public webhook authentication 4 tests, event administration 5 tests including batch rollback, workflow timezone 3 tests including migration/reopen and DST. embedding-host's native HTTP test also runs a queued event, disabled task manually, workflow AI block, exact retry, cancellation, and a scheduled trigger authorized by a different actor through the real scheduler and NativeProvider with a deterministic authenticated local HTTP model endpoint. This does not assert commercial-provider or Kubernetes acceptance.
