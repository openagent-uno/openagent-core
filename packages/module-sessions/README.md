# OpenAgent Sessions module

Optional model-facing session discovery and administration over the runtime's
mandatory `SessionStore`. It does not import or require the memory vault.

The `1.1.0b2` candidate adds model-facing `runs_get`, `runs_events`,
`runs_children`, `runs_wait` and `runs_cancel`. They call the public runtime
state and revalidate the host's current authorization. `runs_wait` accepts one
to eight exact run IDs, waits at most 30 seconds and never cancels execution
when the observer times out. The tools do not mint a delegation, choose another
principal, or send messages to arbitrary sessions. Hosts can omit the module's
`agent_tools` surface or deny individual actions. Result-bearing calls also
check `run.publish` for the turn's verified audience; access to a private
run is not sufficient to quote its output into a shared session.

`Runtime.wait` observes the authoritative store even when the run belongs to
another worker process. A nonlocal active run is not mistaken for a completion;
each observation rechecks current authorization. Cancelling an observer does
not cancel the accepted run.

## Current-conversation recovery (unreleased source candidate)

`sessions_read` accepts an omitted `session_ref` to read this exact current
conversation. Core derives the reference from the verified execution context;
the same read and publication permissions apply. Explicit references retain
their existing meaning, including foreign-reference denial. Use returned
pagination cursors for further messages.

Prompt revision 2 asks the model to recover missing context before saying it
cannot remember an earlier plan, and to distinguish a lossy recap from original
history. It does not load all prior chats into every prompt or assume that
historical permission grants still apply. See [session continuity](../../docs/session-history.md#resuming-a-conversation-and-recovering-omitted-details).
