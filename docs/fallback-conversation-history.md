# Conversation history through provider recovery

When a model cannot serve a turn, an eligible backup can answer using the same
conversation. Both the answer and its tool calls/results must reach the runner's
owned message list before it saves the run. Temporary provider guidance is
removed in place, preserving that list's identity across sync/async and
streaming/non-streaming fallback paths.

Request repair keeps its transformed input private. Only newly generated
messages are retained in the original transcript, including completed tool
evidence on failure or cancellation. Discarding buffered events does not undo
tool effects; the fallback chain stops after a committed tool.

Older completed fallback runs can contain a real user message and displayed
string `content` but no fresh assistant messages. Agent and Team history readers
include that exact recorded text as historical context without rewriting stored
runs. Recovery requires a fresh non-temporary user and no fresh assistant at
all. It excludes compaction, non-completed runs, erased or structured content,
and retains existing member, status, role and history-window filters. It does
not invent tool evidence or attest a new final answer.

Public regressions exercise the actual SDK, Agent/Team, tools and SQLite with a
rate-limited primary, backup, two turns and reopened storage. Separate contracts
cover request repair, interrupted effects and bounded legacy history recovery.
These fixtures do not certify a host deployment or a messaging provider.
