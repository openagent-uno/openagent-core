# Final answers and the complete stream

The additive `final_answer` envelope distinguishes a completed answer from the
complete transcript. It does not change existing `text`, delta, tool, reasoning,
session or attachment history. A quiet channel can acknowledge an incoming
message and deliver this answer once; the app can continue showing all work.

The engine's successful `run.stream` `done` frame contains either:

```json
{
  "kind": "done",
  "text": "I will check. The result is 41.",
  "final_answer": {
    "text": "The result is 41.",
    "parts": [{"kind": "text", "text": "The result is 41."}],
    "attachments": []
  }
}
```

or `final_answer: null` when no authoritative answer boundary is available.
Older frames may omit the field. Neither case authorizes a consumer to extract
the last line, classify paragraphs, strip progress by regex, or substitute the
aggregate `text` as an attested final answer.

The boundary comes from the completed run's terminal structured assistant
message, with no outstanding tool calls and `from_history` false. Agent and Team
completion events carry it separately from their accumulated `content`. Missing
messages, a terminal tool/user message, history-only data or unsupported output
content produce no attestation. Typed text/output-text blocks are supported.
An empty attested message is distinct from an unknown answer.

Native and routed providers forward this boundary through a per-call callback.
There is no provider-global “last answer”. Parent session and run identities
exclude delegated children and unrelated completions. Autoloop iterations clear
the previous boundary. A generate fallback uses its own structured completion;
custom providers can opt into `ModelResponse.final_answer` and the optional
`stream(on_final_answer=...)` callback. Providers that do not opt in remain
compatible and explicitly unattested. Cancellation and failed terminal events
do not publish an attested answer. If the pre-send integrity guard rewrites the
aggregate draft, the old final attestation is invalidated rather than reused.

`Agent.run_stream` carries the raw textual boundary. `AgentExecutor` and
`StreamSession` resolve output using their normal authorization, persistence and
attachment paths before constructing the public envelope. Its `parts` contain
the final text followed only by media explicitly selected by a supported
`[FILE:...]`, `[IMAGE:...]`, `[VOICE:...]` or `[VIDEO:...]` marker in that attested
message. Exact internal marker-to-reference bindings come from the normal
authorized resolution of the complete output; names, hashes and filesystem
path similarity are never used to infer that a file should be sent. A
text-only final therefore has no media, even when earlier narration or a tool
preview referenced an admitted input image. Explicit final forwarding of an
admitted file and generated output remain supported through the same normal
permissions. An unbound or unavailable final marker produces an explicit
unavailable notice, without reading an arbitrary path. Final media retain
their authored order and canonical duplicate refs are sent once. The original
mixed-content transcript keeps its full ordering.
Attachment references are path-free; markers do not grant filesystem access.
Media-only output uses an attested marker-only message; its rendered text is
empty and its selected media remain explicit.

`OutTextFinal`, `BatchedReply`, `StreamCollector.to_legacy_reply()` and the
authorized Sessions live snapshot preserve the field. Snapshot reads resolve
only the references already selected in that final envelope again, so deleted
or no-longer-linked artifacts cannot survive solely inside it. Snapshot
rehydration never repopulates a final answer from full-transcript media.
Product-owned wire encoders must
forward the additive field; this library does not import a product transport.

An internal background completion is not a new human message or proof that
earlier assistant output was delivered. Terminal results already read through
an authorized tool are acknowledged at the catalog's post-publication boundary,
so a later notification does not spuriously reopen the model loop. Unobserved
completions still continue normally. Their internal reminder requires a
self-contained answer to the original task, in the original user's language,
without claiming previous narration was posted unless delivery was actually
confirmed. The final boundary remains the model's actual terminal message;
Core never selects an earlier paragraph by language or prose heuristics.

Deterministic regressions in `tests/public/test_final_answer.py` and
`tests/public/test_final_answer_media_scope.py` cover the real
NativeProvider/runtime tool loop with scripted provider IO, separate parent and
child completions, concurrent calls, fallback, cancellation, guard revision,
Engine and StreamSession delivery, collector failure, and durable SQLite
snapshot/media revocation. These checks do not qualify a real provider or a live
Slack/Discord installation. No package release or host upgrade is implied by
the source change.
`tests/public/test_background_job_observation.py` additionally exercises real
Agent/Executor one- and two-iteration paths, completion/join ordering, scoped
acknowledgement, unrelated jobs and publication revocation.


Core29 public package qualification is recorded in
[the immutable-source public receipt](evidence/core-b29-public-2026-10-23.json):
all 26 anonymous release assets passed full-byte SHA256 verification; a fresh
normal public-wheel installation passed pip check, installed version/prefix and
RECORD checks plus 174 tests and 30 subtests, zero skips. This qualifies the
component boundary, separately from native host adoption or live channel
acceptance.
