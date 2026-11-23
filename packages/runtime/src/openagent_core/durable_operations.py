"""Delegated continuation of registered operations through the runtime ledger."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
import math
import logging
import time
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from .contracts import ExecutionContext, ResourceRef, RunEvent, RunEventPage, RunFinalAnswer, RunRecord, RunRequest
from .operations import STARTED, OperationError, OperationInvocation, StagedOperation, _digest, _resume, resolve_operation, run_operation


@dataclass(frozen=True, slots=True)
class OperationHandoff(OperationInvocation):
    """Explicit host selection of an existing legacy checkpoint for delegation."""

    def __post_init__(self):
        if not self.resume_run_id:
            raise ValueError('A handoff requires its source run')


@dataclass(frozen=True, slots=True)
class DurableOperationPolicy:
    lifetime_seconds: float = 86400
    segment_seconds: float = 21600
    max_segments: int = 8

    def __post_init__(self):
        if (isinstance(self.max_segments, bool) or not isinstance(self.max_segments, int)
                or not 1 <= self.max_segments <= 8):
            raise ValueError('An operation requires one to eight execution segments')
        for value, maximum in ((self.lifetime_seconds, 172800), (self.segment_seconds, 21600)):
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or not 0.1 <= value <= maximum):
                raise ValueError('Operation lifetime and segment duration must be bounded')


@dataclass(frozen=True, slots=True)
class OperationContinuation:
    origin_run_id: str
    name: str
    version: int
    expires_at: float
    segment_seconds: float
    max_segments: int
    segment: int = 0
    previous_run_id: str | None = None
    requested_deadline_seconds: float | None = None
    handoff_run_id: str | None = None
    checkpoint_origin_run_id: str | None = None

    def __post_init__(self):
        if (not self.origin_run_id or not self.name or type(self.version) is not int or self.version < 1
                or not math.isfinite(self.expires_at) or self.expires_at <= 0
                or type(self.segment) is not int or not 0 <= self.segment < self.max_segments
                or bool(self.previous_run_id) != bool(self.segment)
                or bool(self.handoff_run_id) != bool(self.checkpoint_origin_run_id)):
            raise ValueError('Invalid operation continuation')
        DurableOperationPolicy(segment_seconds=self.segment_seconds, max_segments=self.max_segments)


@dataclass(frozen=True, slots=True)
class OperationRunRequest(RunRequest):
    continuation: OperationContinuation | None = None

    def __post_init__(self):
        RunRequest.__post_init__(self)
        if not isinstance(self.continuation, OperationContinuation) or self.steer_run_id is not None:
            raise ValueError('An operation segment requires its continuation descriptor')
        if self.continuation.segment == 0 and self.continuation.origin_run_id != self.run_id:
            raise ValueError('The initial operation segment must own its origin')


@dataclass(frozen=True, slots=True)
class DurableRunRecord(RunRecord):
    origin_run_id: str = ''
    segment_run_id: str = ''
    segment: int = 0


@dataclass(frozen=True, slots=True)
class OperationGrant:
    id: str
    expires_at: float

    def __post_init__(self):
        if (not isinstance(self.id, str) or not self.id or len(self.id) > 300
                or not math.isfinite(self.expires_at) or self.expires_at <= 0):
            raise ValueError('A captured operation grant requires its fixed expiry')


class OperationDelegation(Protocol):
    async def capture(self, request: RunRequest, context: ExecutionContext, *, operation_name: str,
                      operation_version: int, input_digest: str, expires_at: float,
                      handoff_run_id: str | None = None) -> OperationGrant:
        """Capture an idempotent durable grant under the current interactive authority."""
        ...


class _SegmentExecutor:
    def __init__(self, runner):
        self.runner = runner

    async def execute(self, request, context, runtime):
        descriptor = request.continuation
        operation = self.runner.definition(descriptor)
        selected = await resolve_operation(self.runner.resolve_operation, request, context)
        if (selected is None or selected.name != operation.name
                or selected.resume_run_id != descriptor.handoff_run_id):
            raise PermissionError('The admitted operation is no longer selected')
        previous = descriptor.previous_run_id or descriptor.handoff_run_id
        invocation = (OperationInvocation(operation.name, inputs=selected.inputs, resume_run_id=previous)
                      if previous else selected)
        return await run_operation(runtime, request, context, operation, invocation,
                                   self.runner.resolve_operation, self.runner.operation_inference)


class DurableOperationRunner:
    def __init__(self, *, operations, resolve_operation, delegation: OperationDelegation,
                 operation_inference=None, policy=None, poll_seconds=5, now=time.time):
        definitions = tuple(operations)
        if not definitions or any(not isinstance(operation, StagedOperation) for operation in definitions):
            raise TypeError('The runner requires registered staged operation definitions')
        self.operations = {operation.name: operation for operation in definitions}
        if len(self.operations) != len(definitions):
            raise ValueError('Operation names must be unique')
        self.resolve_operation = resolve_operation
        self.delegation = delegation
        self.operation_inference = operation_inference
        self.policy = policy or DurableOperationPolicy()
        if not isinstance(self.policy, DurableOperationPolicy):
            raise TypeError('The runner requires a durable operation policy')
        if not callable(resolve_operation) or not 0.01 <= poll_seconds <= 60:
            raise ValueError('The runner requires a selector and bounded observation interval')
        self.poll_seconds, self.now = poll_seconds, now
        self.runtime = None
        self._lock = asyncio.Lock()
        self._task = None
        self._stopping = False
        self._executor = _SegmentExecutor(self)

    def definition(self, descriptor):
        operation = self.operations.get(descriptor.name)
        if operation is None or operation.version != descriptor.version:
            raise PermissionError('The registered operation definition changed')
        return operation

    async def start(self, runtime):
        if self.runtime is not None:
            raise RuntimeError('The operation runner is already bound')
        for method in ('durable_operation', 'durable_operations', 'stop_durable_operation', 'durable_operation_events'):
            if not callable(getattr(runtime.services.store, method, None)):
                raise TypeError('The runtime store does not support durable operation continuation')
        self.runtime = runtime
        self._stopping = False
        self._task = asyncio.create_task(self._observe(), name='openagent-operation-continuations')

    async def close(self):
        self._stopping = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _observe(self):
        store = self.runtime.services.store
        observe = getattr(store, "observe_operations", None)
        if observe:
            with observe() as changed:
                while not self._stopping:
                    changed.clear()
                    try:
                        await self.tick()
                        timeout = min(60, max(self.poll_seconds, await store.operation_delay(self.now())))
                        try:
                            async with asyncio.timeout(timeout):
                                await changed.wait()
                        except TimeoutError:
                            pass
                    except Exception:
                        logging.getLogger(__name__).warning("Durable operation observation will retry")
                        await asyncio.sleep(self.poll_seconds)
        while not self._stopping:
            await asyncio.sleep(self.poll_seconds)
            try:
                await self.tick()
            except Exception:
                logging.getLogger(__name__).warning('Durable operation observation will retry')

    @staticmethod
    def _context(accepted, agent_id):
        return ExecutionContext(accepted.author, accepted.initiator, accepted.authority,
                                accepted.request.session_id, agent_id, accepted.audience, accepted.scopes,
                                accepted.delegation_id, (), None, accepted.parent_run_id, accepted.deferred)

    async def submit(self, request, context, *, policy=None):
        if isinstance(request, OperationRunRequest):
            raise ValueError('Continuation descriptors are created by the operation runner')
        selected = await resolve_operation(self.resolve_operation, request, context)
        handoff = isinstance(selected, OperationHandoff)
        if selected is None or selected.resume_run_id is not None and not handoff:
            return await self.runtime.submit(request, context)
        operation = self.operations.get(selected.name)
        if operation is None or context.deferred or context.delegation_id or request.steer_run_id:
            raise PermissionError('A new durable operation requires its interactive authority')
        await self.runtime.authorize(context, 'operation.continue',
                                     ResourceRef('session', context.tenant_id, context.session_id), audience=context.audience)
        chosen = policy or self.policy
        async with self._lock:
            existing = await self.runtime.services.store.durable_operation(request.run_id)
            if existing is not None:
                accepted = await self.runtime.accepted_request(request.run_id, context)
                # Compare the caller's complete request and authority without replaying grant capture.
                original = RunRequest(**{field: getattr(accepted.request, field) for field in RunRequest.__dataclass_fields__})
                original = replace(original, deadline_seconds=accepted.request.continuation.requested_deadline_seconds)
                fields = ('author', 'initiator', 'authority', 'session_id', 'agent_id', 'audience', 'scopes', 'parent_run_id')
                restored = self._context(accepted, self.runtime.settings.agent_id)
                if original != request or any(getattr(context, field) != getattr(restored, field) for field in fields):
                    from .contracts import IdempotencyConflict
                    raise IdempotencyConflict('The operation origin belongs to another request or authority')
                return await self.get_run(request.run_id, context)
            checkpoint_origin = None
            if handoff:
                await self.runtime.authorize(context, 'operation.handoff',
                                             ResourceRef('session', context.tenant_id, context.session_id), audience=context.audience)
                source = await self.runtime.get_run(selected.resume_run_id, context)
                accepted = await self.runtime.accepted_request(source.run_id, context)
                fields = ('author', 'initiator', 'authority', 'session_id', 'agent_id', 'audience', 'scopes', 'parent_run_id')
                restored = self._context(accepted, self.runtime.settings.agent_id)
                events = await self.runtime.services.store.operation_events(source.run_id)
                if (source.status not in {'timed_out', 'interrupted'}
                        or accepted.deferred or accepted.delegation_id
                        or isinstance(accepted.request, OperationRunRequest)
                        or any(getattr(context, field) != getattr(restored, field) for field in fields)
                        or not events or events[0].kind != STARTED
                        or events[0].payload.get('name') != operation.name
                        or events[0].payload.get('version') != operation.version):
                    raise PermissionError('The handoff checkpoint does not match this interactive authority and operation')
                try:
                    checkpoint_origin, inputs, _, _, _ = await _resume(
                        self.runtime, self.runtime.services.store, request, context, operation,
                        OperationInvocation(operation.name, inputs=selected.inputs, resume_run_id=source.run_id),
                        self.resolve_operation,
                    )
                except OperationError:
                    raise PermissionError('The handoff checkpoint does not match its canonical operation inputs') from None
                selected = replace(selected, inputs=inputs)
            expires = self.now() + chosen.lifetime_seconds
            binding = {'operation_name': operation.name, 'operation_version': operation.version,
                       'input_digest': _digest(selected.inputs), 'expires_at': expires}
            if handoff:
                binding['handoff_run_id'] = selected.resume_run_id
            grant = await self.delegation.capture(request, context, **binding)
            if not isinstance(grant, OperationGrant) or not self.now() < grant.expires_at <= expires:
                raise PermissionError('The host did not capture a durable operation delegation')
            delegated = replace(context, deferred=True, delegation_id=grant.id, capabilities=(), ingress_id=None)
            descriptor = OperationContinuation(request.run_id, operation.name, operation.version, grant.expires_at,
                                               chosen.segment_seconds, chosen.max_segments,
                                               requested_deadline_seconds=request.deadline_seconds,
                                               handoff_run_id=selected.resume_run_id if handoff else None,
                                               checkpoint_origin_run_id=checkpoint_origin)
            segment = OperationRunRequest(request.run_id, request.session_id, request.idempotency_key, request.input,
                                          min(chosen.segment_seconds, grant.expires_at-self.now()), request.attachments,
                                          request.model_ref, continuation=descriptor)
            return await self.runtime.execute_operation(segment, delegated, self._executor)

    async def get_run(self, run_id, context):
        original = await self.runtime.get_run(run_id, context)
        row = await self.runtime.services.store.durable_operation(run_id)
        if row is None or run_id != row['origin_run_id']:
            return original
        current = await self.runtime.get_run(row['current_run_id'], context)
        state = current.status
        if row['state'] == 'active' and state in {'timed_out', 'interrupted'}:
            state = 'running'
        elif row['state'] == 'cancelled':
            state = 'cancelled'
        elif row['state'] in {'expired', 'exhausted', 'revoked'}:
            state = 'failed'
        output = current.output if current.terminal and state == current.status else None
        if row['state'] in {'expired', 'exhausted', 'revoked'}:
            output = {'reason': 'operation_' + row['state'], 'retry_allowed': False}
        return DurableRunRecord(row['origin_run_id'], original.session_id, original.tenant_id, state,
                                original.request_digest, output,
                                row['state'] == 'cancelled', original.parent_run_id,
                                row['origin_run_id'], current.run_id, row['segment'])

    async def cancel(self, run_id, context, *, reserve=False):
        try:
            original = await self.runtime.get_run(run_id, context)
        except LookupError:
            return await self.runtime.cancel(run_id, context, reserve=reserve)
        row = await self.runtime.services.store.durable_operation(run_id)
        if row is None:
            return await self.runtime.cancel(run_id, context, reserve=reserve)
        await self.runtime.authorize(context, 'run.cancel',
                                     ResourceRef('session', original.tenant_id, original.session_id), audience=context.audience)
        from .persistence import complete_before_cancelling
        return await complete_before_cancelling(self._cancel_operation(run_id, context))

    async def _cancel_operation(self, run_id, context):
        async with self._lock:
            row = await self.runtime.services.store.durable_operation(run_id)
            current = await self.runtime.get_run(row['current_run_id'], context)
            if current.terminal and current.status not in {'timed_out', 'interrupted'}:
                return await self.get_run(row['origin_run_id'], context)
            await self.runtime.services.store.stop_durable_operation(row['origin_run_id'], 'cancelled')
            await self.runtime.cancel(row['current_run_id'], context)
        return await self.get_run(row['origin_run_id'], context)

    async def events(self, run_id, after, context):
        await self.runtime.authorize_replay(run_id, context)
        store = self.runtime.services.store
        row = await store.durable_operation(run_id)
        if row is None or run_id != row['origin_run_id']:
            return await self.runtime.events(run_id, after, context)
        rows = await store.durable_operation_events(run_id, after)
        for ident in {event.run_id for event in rows}:
            await self.runtime.authorize_replay(ident, context)
        return self._origin_events(run_id, rows)

    async def events_page(self, run_id, after, context, *, limit=1000):
        RunEventPage.validate(after, limit)
        await self.runtime.authorize_replay(run_id, context)
        row = await self.runtime.services.store.durable_operation(run_id)
        if row is None or run_id != row['origin_run_id']:
            return await self.runtime.events_page(run_id, after, context, limit=limit)
        store = self.runtime.services.store
        window = getattr(store, 'durable_operation_event_window', None)
        rows = await window(run_id, after, limit + 1) if window else await store.durable_operation_events(run_id, after)
        rows = rows[:limit + 1]
        for ident in {event.run_id for event in rows}:
            await self.runtime.authorize_replay(ident, context)
        return RunEventPage.from_window(self._origin_events(run_id, rows), after, limit)

    @staticmethod
    def _origin_events(origin_id, events):
        result = []
        for event in events:
            kind = event.kind
            if kind in {'run.timed_out', 'run.interrupted'}:
                kind = 'run.operation.segment.' + kind.removeprefix('run.')
            elif kind == 'run.operation.finished':
                kind = 'run.' + event.payload['status']
            result.append(RunEvent(event.cursor, origin_id, kind,
                                   {**event.payload, 'segment_run_id': event.run_id}))
        return tuple(result)

    async def final_answer(self, run_id, context):
        await self.runtime.authorize_replay(run_id, context)
        record = await self.get_run(run_id, context)
        segment_id = getattr(record, 'segment_run_id', run_id)
        if record.status != 'success':
            return RunFinalAnswer(run_id, segment_id, None)
        answer = await self.runtime.final_answer(segment_id, context)
        return replace(answer, run_id=run_id)

    async def tick(self):
        if self.runtime is None or self.runtime._closing or not self.runtime._started:
            return
        async with self._lock:
            store = self.runtime.services.store
            ready = getattr(store, "ready_operations", None)
            rows = await ready(self.now()) if ready else await store.durable_operations()
            for row in rows:
                current = await self.runtime.services.store.get(row['current_run_id'])
                if current is None or not current.terminal:
                    continue
                if current.status not in {'timed_out', 'interrupted'}:
                    await self.runtime.services.store.stop_durable_operation(row['origin_run_id'], current.status)
                    continue
                accepted = await self.runtime.services.store.accepted_request(current.run_id)
                descriptor = accepted.request.continuation
                remaining = descriptor.expires_at - self.now()
                if remaining <= 0 or row['segment'] + 1 >= descriptor.max_segments:
                    await self.runtime.services.store.stop_durable_operation(row['origin_run_id'],
                                                                             'expired' if remaining <= 0 else 'exhausted')
                    continue
                if (isinstance(current.output, dict) and current.output.get('reason') == 'operation_pending'
                        and self.now() < row['segment_finished_at_ms'] / 1000 + current.output['retry_after_seconds']):
                    continue
                segment = row['segment'] + 1
                next_id = str(uuid5(NAMESPACE_URL, f'openagent-operation:{row["origin_run_id"]}:{segment}'))
                request = replace(accepted.request, run_id=next_id,
                                  idempotency_key=f'operation:{row["origin_run_id"]}:{segment}',
                                  deadline_seconds=min(descriptor.segment_seconds, remaining),
                                  continuation=replace(descriptor, segment=segment, previous_run_id=current.run_id))
                context = self._context(accepted, self.runtime.settings.agent_id)
                try:
                    self.definition(descriptor)
                    await self.runtime.execute_operation(request, context, self._executor)
                except PermissionError:
                    await self.runtime.services.store.stop_durable_operation(row['origin_run_id'], 'revoked')
