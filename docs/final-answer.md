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
the final text followed by already-authorized output media in their original
media order. The original mixed-content transcript keeps its full ordering.
Attachment references are path-free; markers do not grant filesystem access.
Media-only output is supported when the run supplies an attested empty message.

`OutTextFinal`, `BatchedReply`, `StreamCollector.to_legacy_reply()` and the
authorized Sessions live snapshot preserve the field. Snapshot reads resolve
attachment references again, so deleted or no-longer-linked artifacts cannot
survive solely inside a nested final envelope. Product-owned wire encoders must
forward the additive field; this library does not import a product transport.

Deterministic regressions in `tests/public/test_final_answer.py` cover the real
NativeProvider/runtime tool loop with scripted provider IO, separate parent and
child completions, concurrent calls, fallback, cancellation, guard revision,
Engine and StreamSession delivery, collector failure, and durable SQLite
snapshot/media revocation. These checks do not qualify a real provider or a live
Slack/Discord installation. No package release or host upgrade is implied by
the source change.
