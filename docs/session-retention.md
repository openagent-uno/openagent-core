# Session retention and interactive conversations

`openagent_core.core.session_retention` keeps the SQLite store bounded. It reads
the `retention:` block of `openagent.yaml`:

| Key | Default | Effect |
| --- | --- | --- |
| `days` | 3 | A legacy `sessions` row idle longer than this is deleted, which tombstones its conversation. This is the only step that removes an interactive conversation. Conversational prefixes (`tg:`, `peer:`, `agent-brid`) are exempt. |
| `max_runs`, `max_bytes`, `trim_threshold_bytes` | 5, 400000, 200000 | Long transcripts are trimmed to their newest runs; the newest run is never dropped. |
| `scheduler_days` | 7 | Run-logs of scheduled runs and automations (`scheduler:*`, `automation:*`) are deleted once idle this long. |
| `automation_days` / `automation_hours` | 1 / 0 | Native 1.1 automation sessions are tombstoned once idle. |

## Why the run-log step never touches conversations

On OpenAgent 1.1 a conversation with the user also has a legacy `sessions` row
with `session_type='agent'`. Deleting that row makes the server tombstone the
conversation, which then disappears from the app and later loses its messages.
The run-log step therefore matches only `scheduler:*` and `automation:*` ids,
never `session_type`.

An oversized run-log (more than 500 KB of `runs`) is trimmed to its newest run
instead of being deleted, so a large run still frees the writer without losing
the latest transcript; the run's full messages stay in the session for
`scheduler_days`, which lets a nightly review of the run history read recent runs.

## Restoring a conversation deleted by mistake

A tombstone keeps the conversation's messages and tool calls until payload
pruning reclaims them (pruning runs only for tombstoned sessions that have no
legacy `sessions` row). Until then
`openagent_core.memory.operational.repository.restore_tombstoned_session(conn, session_id)`
brings the conversation back unchanged: it clears the tombstone, restores the
real last-activity time, revives the activity row, queues search upserts and
records a `session.restored` event. It runs inside the caller's transaction and
never rebuilds history from the legacy row, whose runs may already be compacted.

It refuses with `SessionRestoreError` when the payloads were pruned
(`payloads_pruned`) or when a legacy-projected session has lost its `sessions`
row (`legacy_source_missing`), because the next reconciliation would tombstone
it again. For a runtime-owned conversation the legacy row is the provider's
conversation buffer: putting it back, for example from a database backup,
after the restore lets the model resume with its earlier context. Stop the
retention job first, and back up the database before writing to it.
