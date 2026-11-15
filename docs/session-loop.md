# Session loops

A session loop is a scheduled task that runs *inside* one conversation: every
firing is a turn of that chat, sees its history and answers there.

## Creating one

- Hosts: `scheduled_tasks.target_session_id` (`MemoryDB.add_task(...,
  target_session_id=...)`). The standalone server exposes it as `session_id` on
  `POST /api/scheduled-tasks` (admin access to the conversation required) and
  lists a conversation's loops with `GET /api/scheduled-tasks?session_id=`.
- The agent: `create_scheduled_task(..., in_this_conversation=True)` binds the
  task to the calling turn's own conversation (taken from the execution
  context, never from a model-supplied id). It is refused from automation and
  scheduler runs.
- The app: `/loop <interval> <prompt>`, `/loop`, `/loop stop`, and the Loops
  section of the session panel.

## How a firing runs

`Scheduler._execute_task` sees `target_session_id` and calls `_run_in_session`:

- Under the public runtime the firing is a top-level run of the target session
  (`runtime.submit` + `wait`), admitted with the task's captured delegation
  (`context` with the target `session_id` and no parent run). It is never a
  child run: a child would re-parent the user's conversation, which the run
  store refuses. The runtime serializes it behind any live turn of the session.
- Hosts without the public runtime run a plain `agent.run` in the session.
- `task_runs.session_id` links each firing to the conversation; a non-successful
  turn is recorded with its status.
- When the turn ends, a `turn_complete` frame for the session goes out through
  the host's broadcast sink for detached runs (`set_child_broadcast_sink`),
  with the outcome (`completed`, `empty`, `error`, `cancelled`) and no content.
  A client that has the conversation open reloads it through its history API,
  so the loop's turn appears without reopening the chat; other clients ignore
  it. Hosts without a sink stay silent.

The task definition's digest covers `target_session_id`, so the person who
approved the task approved that conversation. Hosts that digest definitions
should ignore the field while it is unset, so adding the column does not
invalidate existing approvals.
