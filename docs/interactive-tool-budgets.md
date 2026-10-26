# Interactive tool budgets

Interactive document and application work can legitimately need more than sixty
tool calls, including discovery, inspection, editing, rendering and delivery.
Core no longer silently applies a sixty-call ceiling when the host has not
configured one. This prevents a completed document from losing its delivery
step solely because preparation consumed that implicit allowance.

An explicit execution policy `max_tool_calls` remains authoritative, including
its existing validation, narrowing and cached-runner separation. Operators can
also set `OPENAGENT_MAX_TOOL_CALLS_PER_RUN` to a positive integer. Zero and
negative values retain their existing meaning of no call-count ceiling. A
nonempty invalid setting retains the historical sixty-call fallback; it does
not silently disable an intended bound. Lean background events and tasks keep
their existing limits of ten and forty, respectively. Host deadlines,
cancellation, authorization, attachment limits and capability scopes are
unchanged.

`tests/public/test_long_tool_runs.py` exercises the real native runner with
scripted provider responses. The ordinary case executes sixty-one preparation
tools and a final delivery tool. Explicit host and operator limits still stop
execution at three calls. This is deterministic source acceptance, separate
from live provider and channel qualification.
