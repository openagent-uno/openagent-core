# Durable operation continuation

This beta124 contract lets a trusted host opt selected staged operations
into delegated continuation. Core observes the existing runtime ledger and
admits another bounded execution segment after a timeout or runtime interruption.
Each segment uses the original accepted input, operation definition and domain
idempotency keys. Ordinary interactive runs retain their current lifecycle.

```python
from openagent_core.durable_operations import (
    DurableOperationPolicy, DurableOperationRunner, OperationGrant,
)

runner = DurableOperationRunner(
    operations=registered_operations,
    resolve_operation=select_operation,
    operation_inference=configured_inference,
    delegation=host_delegations,
    policy=DurableOperationPolicy(
        lifetime_seconds=86400, segment_seconds=21600, max_segments=8,
    ),
)
# Include runner after the agent executor in Runtime(..., modules=(executor, runner)).
# Supply the host delegation service through RuntimeServices(delegations=...).
app = create_app(runtime, resolve_context, durable_operations=runner)
```

The host's pure selector must return the same initial `OperationInvocation`
for the original request. A selector result of `None` uses ordinary chat.
Explicit legacy resume requests also retain the interactive resume contract.
The operation registry and selector must remain available across restart.
Changing the registered definition version prevents automatic continuation.

## Capturing authority

`OperationDelegation.capture(request, context, *, operation_name,
operation_version, input_digest, expires_at, handoff_run_id=None)` captures an explicit host grant
under the live interactive authority and returns `OperationGrant(id, expires_at)`.
Capture must be idempotent for the full original request and execution identity,
definition and input digest. A lost admission must return the original grant
and its original expiry. Retrying must never extend it. The effective expiry
must be positive, unexpired and no later than the requested expiry.

The runner first authorizes `operation.continue` on the session. It admits the
initial segment through `Runtime.execute_operation` with the original author,
initiator, authority, session, audience and scopes, a captured delegation ID,
`deferred=True`, and empty temporary capabilities. The host's existing
`DelegationService.validate` and optional credential scope validate that grant
throughout execution and on every continuation. Credentials and capability
leases are never stored as reusable authority. The grant should restrict the
operation, its input and authorized resources, in addition to the session.

The grant service owns durable grant storage and revocation policy. The runner
and scheduling logic belong in Core. Consumers supply domain data, identity and
authorization adapters. They do not copy the runner or schedule synthetic chat
messages to resume operations.

## Atomic admission and recovery

The SQLite adapter records a logical origin cursor in the same transaction as
the segment's ordinary run acceptance. Segment IDs and keys are deterministic.
An interrupted initial segment that had not started its first checkpoint can
recover under the same logical origin. The existing staged-operation checks
protect completed steps and uncertain external effects. Only a read or a tool
declared idempotent may replay its uncompleted intent. A failed segment ends
the operation. An uncertain non-idempotent effect requires reconciliation.

The lifetime is bounded to two days, each segment to six hours, and the chain
to eight segments. Receipt waits retain their own step deadline and poll limit.
An expired lifetime, exhausted segment count or revoked grant ends continuation.
At most 100 logical operations can be active in one runtime store. The cursor
contains run IDs and lifecycle state. Inputs, checkpoints and results continue
to belong to the runtime run ledger. Session deletion cascades to its cursor.

`runner.start` and `runner.close` follow Runtime's module lifecycle. Closing
the runtime leaves work interrupted for authorized recovery. It does not mark
it cancelled. An operation cancellation is persisted before stopping the current
segment, and prevents a later restart from creating another segment.

## Gateway and SDK

The optional `durable_operations=runner` argument on `create_app` uses the
standard submit, read, event and cancel routes. Wire callers cannot supply
delegations or continuation descriptors. Those are created only by the host's
runner. The original run ID remains the logical status endpoint.

A logical read returns `origin_run_id`, `segment_run_id` and `segment` alongside
the ordinary run fields. Its status stays `running` between resumable segments
and carries the final segment's output when it succeeds. Reading a physical
segment returns that exact run and its verified `origin_run_id`. Domain asset
adapters can check this provenance when an artifact belongs to a later segment.
They must verify that the origin belongs to the authorized project or resource.

Logical events retain the store's monotonic global cursor. They identify the
physical segment in `payload.segment_run_id`. Intermediate timeout and
interruption events use `run.operation.segment.*`, so they do not report the
logical operation as finished. Reading a physical segment retains its original
events. Expiry, exhaustion, revocation and cancellation append an idempotent
logical terminal event atomically with stopping the cursor; event-only clients
receive `run.failed` or `run.cancelled`. The Python and TypeScript SDKs expose
the extra provenance fields.

`runner.cancel(origin_or_segment, context)` authorizes cancellation and stops
the entire logical operation. Cancelling a completed operation preserves its
terminal result. Hosts using another transport should use these same public
submit, get, events and cancel methods.

## Explicit handoff of an existing checkpoint

An existing interactive operation cannot acquire reusable authority implicitly.
A host may explicitly select
`OperationHandoff(name, inputs=original_inputs, resume_run_id=source_run_id)`
instead of an ordinary `OperationInvocation`. The runner additionally authorizes
`operation.handoff` on the exact session before grant capture. The source must
be a nondelegated legacy operation in that session, timed out or interrupted,
with the same original author, initiator, authority, audience, scopes and parent,
and matching definition version and input digest. Capture must bind the source
run ID as well as the new full request and identity.

Admission claims that source checkpoint atomically and exactly once. The
ordinary checkpoint writer also respects this claim, preventing a concurrent
legacy resume from forking after a crash before the handoff's first checkpoint.
The new request owns the logical origin; `checkpoint_origin_run_id` preserves
the old operation origin and its domain idempotency keys. Existing receipts and
completed steps are resumed, without submitting a replacement external job.
No legacy request, checkpoint or execution authority is rewritten. An uncertain
effect still fails closed. The complete ancestry retains its existing limit.

## Pending readiness versus failed or uncertain effects

A trusted readiness adapter can raise
`OperationPending(retry_after_seconds=15)` when a read-only readiness observation
has not completed and no external effect has been submitted. This public signal
is distinct from a generic provider error. Its delay is bounded from 0.1 seconds
to one hour. Core records a timed-out segment with `reason=operation_pending`
and the retry delay. The runner waits that delay, revalidates authority and
continues under the same total lifetime and segment budget.

The step must independently satisfy the existing replay contract. A pending
signal from a non-idempotent `ToolStep` becomes a failure and cannot authorize
retrying an effect. A visual completion's evidence tool must also be idempotent.
Adapters must not translate arbitrary provider errors, failed receipts, model
schema failures or unknown submission outcomes into pending readiness. A raw
`TimeoutError` already maps to a timed-out segment; generic failures terminate
the operation. No interpretation of error strings grants retry authority.

Source tests exercise timeout continuation without another user message,
restart with and without a checkpoint, one domain submission across segments,
revocation, lifetime and segment bounds, cancellation, authority replay,
uncertain effects, lost admission and authenticated gateway/SDK polling.
Live provider and consumer acceptance are separate qualification steps.

## Release and adoption

The beta124 cohort contains the kernel, SQLite, gateway and Python SDK changes
together. Publication requires the official Core release qualification gates.
Build the clean candidate with `packaging/build.py`, then run
`packaging/qualify.py` against that immutable wheel manifest and an independent
environment, as described in [USAGE](USAGE.md#reproducible-engine-qualification).
Reserve a fresh version against the current upstream before publication; never
replace existing release assets. Hosts install the versioned released cohort
and opt in through the public runner, selector and delegation ports above.
Source or installed-fixture evidence does not establish consumer/provider
production acceptance.
