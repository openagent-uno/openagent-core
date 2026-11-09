# Staged operations in authenticated runs

Starting with Core `1.1.0b58`, an embedding host can register versioned
operation definitions on `AgentExecutor`.
The host selects one from a verified `RunRequest` and its `ExecutionContext`.
Ordinary requests still use the agent's chat path. Selection runs inside the
normal request scope and cannot change the admitted run's authority or session.

```python
from openagent_core.engine import AgentExecutor
from openagent_core.inference import StatelessInference
from openagent_core.operations import (
    CompleteOperation, OperationInvocation, StagedOperation, ToolStep,
)

def next_step(state):
    if 'submit' in state.results:
        return CompleteOperation('The request has been accepted.')
    return ToolStep('submit', 'records', 'records_submit', {
        'session': state.session_id,
        'key': state.idempotency_key('submit'),
        'payload': state.inputs['payload'],
    }, idempotent=True)

def select(request, context):
    # A trusted host parses its admitted request and validates the input.
    selected = resolve_verified_request(request, context)
    return OperationInvocation('submit-record', selected) if selected is not None else None

executor = AgentExecutor(
    agent,
    operations=(StagedOperation('submit-record', 1, next_step),),
    resolve_operation=select,
    operation_inference=StatelessInference(configured_model),
)
```

`next_step` is a synchronous data-mapping function. It receives a detached
snapshot of the original inputs, completed results and the run that produced
each result (`result_run_ids`). It returns the next declared stage, without
calling tools, starting tasks or mutating storage. Increase the definition
version when a stage's behavior changes.

- `ToolStep` invokes an exact authorized catalog destination. It checkpoints
  the JSON result envelope and stops when the tool returns an MCP error.
- `WaitStep` polls one exact read-only tool with fixed arguments, a deadline and
  a maximum of 128 reads. The host supplies a pure terminal predicate. Use it
  for receipts of already submitted jobs.
- `CompletionStep` invokes the configured `StatelessInference` provider.
  It supplies the stage prompt and JSON Schema with no chat transcript or
  tools. The engine parses and validates the output before checkpointing it.
  An optional `video_tool` must return one authorized embedded MP4. The bound
  FFmpeg extractor supplies chronological images and timestamps. A missing or
  unreviewable video fails the stage before inference. The video is retained
  through the canonical tool receipt. The stage stores only the validated JSON.
  Starting with Core `1.1.0b59`, bind `media.operation_video_frame_extractor`
  to configure resolution and sample positions independently for these steps.
  If that binding is absent, the operation uses `media.tool_video_frame_extractor`.
  Starting with Core `1.1.0b61`, `image_tool` accepts one exact authorized
  catalog tool that returns between one and 24 inline MCP image blocks, with
  optional text in the same order.
  Evidence must be JPEG, PNG or WebP, with at most 8 MiB of decoded image data
  in total. A missing image, invalid inline data or linked resource fails the
  stage before inference. The canonical tool receipt retains the evidence and
  the stage checkpoints the validated JSON. Choose at most one evidence tool
  per completion step.
  The default is one inference attempt. Starting with Core `1.1.0b63`, set
  `max_attempts` to two or three to retry output that fails JSON or schema
  validation. Attempts share the
  step deadline and use the same authorized evidence fetched once. The
  provider receives a formatting reminder on retry, without earlier model
  output. Provider failures and cancellation follow the enclosing run policy.
- `CompleteOperation` returns ordinary final answer text. The plan must only
  select publish stages after its relevant business checks, and the domain
  publish tool should revalidate them at the mutation boundary.

Canonical tool receipts retain the normal catalog source, invocation and result
authorization. A bounded stage intent is persisted before execution and the
result is checkpointed after it returns. The SQLite runtime store implements
the optional `StagedOperationStore` port on its existing event table. Existing
gateway clients submit, wait, read and cancel the ordinary run unchanged.

An interrupted, timed-out or failed operation can continue via a new run
selected with `OperationInvocation(name, resume_run_id=previous_run_id)`.
The engine verifies the same execution authority, session, definition version
and original accepted input. The selector is called on the original accepted
request to reconstruct inputs. Each prior run can be continued once, and a
continuation uses current permissions. A completed stage is not replayed.
If a tool stage has an intent without a completed result, it can be attempted
again only when the host declared the exact stage idempotent. Supply the
stable `state.idempotency_key(step)` to a domain tool that supports it. Mark
read-only evidence tools idempotent to replay an interrupted visual stage. Manual
cancellation is terminal. Restart preserves the checkpoints and leaves the
old run interrupted until a new authorized request continues it.

This interface caps operation results at 256 KiB per stage and 4 MiB in total,
with at most 256 completed stages and eight continuation links. Individual
step deadlines range from 0.1 to 3,600 seconds, under the enclosing run deadline.
Stateless planning receives its explicit prompt and any declared visual evidence.
