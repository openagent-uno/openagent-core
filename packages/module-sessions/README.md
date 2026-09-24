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
