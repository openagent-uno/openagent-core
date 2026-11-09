"""Checkpointed stages chosen by a trusted host and executed by OpenAgent."""
from __future__ import annotations

import asyncio
import copy
from dataclasses import dataclass, field
from hashlib import sha256
import inspect
import json
import math
import re
from types import MappingProxyType
from typing import Any, Callable, Mapping, Protocol

from .contracts import ExecutionContext, ResourceRef, RunEvent, RunRequest, canonical_json


STARTED = 'run.operation.started'
STEP_STARTED = 'run.operation.step.started'
STEP_COMPLETED = 'run.operation.step.completed'
CHECKPOINT_EVENTS = (STARTED, STEP_STARTED, STEP_COMPLETED)
_MAX_STEPS = 256
_MAX_RESUMES = 8
_MAX_POLLS = 128
_MAX_VALUE_BYTES = 256 * 1024
_MAX_RESULTS_BYTES = 4 * 1024**2
_MAX_PROMPT_BYTES = 64 * 1024
_RESUMABLE = frozenset({'failed', 'interrupted', 'timed_out'})
_KEY = re.compile(r'[A-Za-z0-9_.:-]{1,120}')


class OperationError(RuntimeError):
    """An operation cannot safely advance."""


class StagedOperationStore(Protocol):
    """Optional RuntimeStore port, used only by staged operations."""

    async def start_operation(self, run_id: str, payload: Mapping[str, Any]) -> RunEvent:
        """Atomically append STARTED and consume any prior resume checkpoint once."""
        ...

    async def operation_events(self, run_id: str) -> tuple[RunEvent, ...]:
        """Read only CHECKPOINT_EVENTS, in cursor order."""
        ...


@dataclass(frozen=True, slots=True)
class OperationInvocation:
    name: str
    inputs: Mapping[str, Any] | None = None
    resume_run_id: str | None = None


@dataclass(frozen=True, slots=True)
class OperationState:
    origin_run_id: str
    run_id: str
    session_id: str
    inputs: Mapping[str, Any]
    results: Mapping[str, Any]
    result_run_ids: Mapping[str, str]

    def idempotency_key(self, step: str) -> str:
        """Stable key for a domain operation, including across a resume."""
        _check_key(step)
        return 'operation-' + _digest([self.origin_run_id, step])


@dataclass(frozen=True, slots=True)
class ToolStep:
    key: str
    source_id: str
    tool_name: str
    args: Mapping[str, Any] = field(default_factory=dict)
    idempotent: bool = False
    timeout_seconds: float = 60


@dataclass(frozen=True, slots=True)
class WaitStep:
    key: str
    source_id: str
    tool_name: str
    args: Mapping[str, Any]
    until: Callable[[Any], bool]
    timeout_seconds: float = 600
    interval_seconds: float = 2


@dataclass(frozen=True, slots=True)
class CompletionStep:
    key: str
    prompt: str
    schema: Mapping[str, Any]
    video_tool: ToolStep | None = None
    timeout_seconds: float = 120
    image_tool: ToolStep | None = None
    max_attempts: int = 1

    def __post_init__(self):
        if self.video_tool is not None and self.image_tool is not None:
            raise ValueError('A completion requires at most one evidence tool')
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 3:
            raise ValueError('A completion requires one to three attempts')


@dataclass(frozen=True, slots=True)
class CompleteOperation:
    output: str


OperationStep = ToolStep | WaitStep | CompletionStep | CompleteOperation


@dataclass(frozen=True, slots=True)
class StagedOperation:
    name: str
    version: int
    next_step: Callable[[OperationState], OperationStep]

    def __post_init__(self):
        _check_key(self.name)
        if type(self.version) is not int or self.version < 1 or not callable(self.next_step):
            raise ValueError('An operation requires a positive version and a stage selector')


def _check_key(key: str) -> None:
    if not isinstance(key, str) or not _KEY.fullmatch(key):
        raise ValueError('Operation keys must be bounded identifiers')


def _encode(value: Any, limit: int = _MAX_VALUE_BYTES) -> bytes:
    data = canonical_json(value).encode()
    if len(data) > limit:
        raise OperationError('Operation data exceeds its byte limit')
    return data


def _digest(value: Any) -> str:
    return sha256(_encode(value)).hexdigest()


def _seconds(value: float, *, max_seconds: int = 3600) -> None:
    if (not isinstance(value, (int, float)) or isinstance(value, bool)
            or not math.isfinite(value) or not 0.1 <= value <= max_seconds):
        raise ValueError('Operation deadlines and intervals must be bounded seconds')


def _exact_tool(step: ToolStep | WaitStep) -> list:
    if any(not isinstance(value, str) or not 0 < len(value) <= 256 or value != value.strip()
           for value in (step.source_id, step.tool_name)) or not isinstance(step.args, Mapping):
        raise ValueError('An operation tool needs an exact source, name and argument object')
    return [step.source_id, step.tool_name, dict(step.args)]


def _step_digest(step: ToolStep | WaitStep | CompletionStep) -> str:
    _check_key(step.key)
    _seconds(step.timeout_seconds)
    if isinstance(step, ToolStep):
        if type(step.idempotent) is not bool:
            raise ValueError('Tool replay must be declared explicitly')
        return _digest(['tool', *_exact_tool(step), step.idempotent, step.timeout_seconds])
    if isinstance(step, WaitStep):
        _seconds(step.interval_seconds, max_seconds=60)
        if not callable(step.until):
            raise TypeError('A wait step requires a terminal-state predicate')
        return _digest(['wait', *_exact_tool(step), step.timeout_seconds, step.interval_seconds])
    if not isinstance(step.prompt, str) or not step.prompt.strip() or len(step.prompt.encode()) > _MAX_PROMPT_BYTES:
        raise ValueError('A completion requires a bounded prompt')
    if not isinstance(step.schema, Mapping):
        raise TypeError('A completion requires a JSON schema')
    if step.video_tool is not None and not isinstance(step.video_tool, ToolStep):
        raise TypeError('Completion video requires an exact tool descriptor')
    video = _step_digest(step.video_tool) if step.video_tool is not None else None
    data = ['completion', step.prompt, dict(step.schema), video, step.timeout_seconds]
    if step.image_tool is not None:
        if not isinstance(step.image_tool, ToolStep):
            raise TypeError('Completion images require an exact tool descriptor')
        data.append(['image', _step_digest(step.image_tool)])
    if step.max_attempts != 1:
        data.append(['max_attempts', step.max_attempts])
    return _digest(data)


def _safe_to_retry(step: ToolStep | WaitStep | CompletionStep) -> bool:
    if isinstance(step, ToolStep):
        return step.idempotent
    if isinstance(step, CompletionStep):
        evidence = step.video_tool or step.image_tool
        return evidence is None or evidence.idempotent
    return True


async def _call(runtime, step: ToolStep | WaitStep) -> Any:
    from .core.tool_scope import current_tool_allowlist, normalize_family
    from .engine import call_tool
    from .mcp.servers.tool_search.adapters import coerce_mcp_result_to_jsonable

    families = current_tool_allowlist()
    if families is not None and normalize_family(step.source_id) not in families:
        raise PermissionError('Operation tool is outside this run’s allowed families')
    result = await call_tool(runtime.capabilities, step.source_id, step.tool_name, dict(step.args))
    envelope = coerce_mcp_result_to_jsonable(result)
    if isinstance(envelope, dict) and envelope.get('isError'):
        raise OperationError('An operation tool returned an error')
    return envelope


def _json_object(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError('Duplicate JSON properties')
    return result


def _structured_output(response, validator):
    content = getattr(response, 'content', None)
    if not isinstance(content, str) or len(content.encode()) > _MAX_VALUE_BYTES:
        raise OperationError('Structured completion exceeded its output limit')
    try:
        value = json.loads(content, object_pairs_hook=_json_object)
        _encode(value)
    except (ValueError, RecursionError):
        raise OperationError('Structured completion did not return exact JSON') from None
    if not validator.is_valid(value):
        raise OperationError('Structured completion did not match its schema')
    return value


def _image_evidence(envelope):
    from .core._runner.utils.mcp import _MCP_CONTENT_MAX_ITEMS, _decode_mcp_base64
    from .core._runner.utils.media import get_image_type
    from .inference import InferenceImage, MAX_INFERENCE_IMAGES, MAX_INFERENCE_IMAGE_BYTES

    blocks = envelope.get('content') if isinstance(envelope, dict) else None
    if not isinstance(blocks, list) or len(blocks) > _MCP_CONTENT_MAX_ITEMS:
        raise OperationError('The authorized image evidence exceeds its content limit or is unavailable')
    text, images = [], []
    remaining = MAX_INFERENCE_IMAGE_BYTES
    text_bytes = 0
    for block in blocks:
        if not isinstance(block, dict):
            raise OperationError('Image evidence requires inline images and text')
        if block.get('type') == 'text' and isinstance(block.get('text'), str):
            line = block['text']
        elif block.get('type') == 'image' and len(images) < MAX_INFERENCE_IMAGES:
            mime = block.get('mimeType')
            try:
                data = _decode_mcp_base64(block.get('data'), label='MCP image', remaining_bytes=remaining)
                image = InferenceImage(data, mime)
                if mime != f'image/{get_image_type(data)}':
                    raise ValueError('Image bytes do not match the declared media type')
            except (TypeError, ValueError):
                raise OperationError('The authorized tool returned invalid or oversized image data') from None
            remaining -= len(data)
            images.append(image)
            line = f'[Image {len(images)}]'
        else:
            raise OperationError('Image evidence requires a bounded list of inline images and text')
        text_bytes += len(line.encode()) + 1
        if text_bytes > _MAX_PROMPT_BYTES:
            raise OperationError('Operation image evidence exceeds the prompt bound')
        text.append(line)
    if not images:
        raise OperationError('The authorized tool returned no inline images')
    return '\n'.join(text), tuple(images)


async def _complete(runtime, inference, step: CompletionStep) -> Any:
    from jsonschema import Draft202012Validator
    from .inference import InferenceImage, InferenceRequest

    if inference is None:
        raise OperationError('No stateless inference provider was configured')
    schema = dict(step.schema)
    Draft202012Validator.check_schema(schema)
    prompt, images = step.prompt, ()
    if step.image_tool is not None:
        async with asyncio.timeout(step.image_tool.timeout_seconds):
            envelope = await _call(runtime, step.image_tool)
        text, images = _image_evidence(envelope)
        prompt += '\n\nAuthorized image evidence, in attachment order:\n' + text
    elif step.video_tool is not None:
        from .mcp.servers.tool_search.adapters import _embedded_video, _model_visible_result

        extractor = runtime.service('media.operation_video_frame_extractor')
        if extractor is None:
            extractor = runtime.service('media.tool_video_frame_extractor')
        if extractor is None:
            raise OperationError('No authorized video extractor was configured')
        async with asyncio.timeout(step.video_tool.timeout_seconds):
            envelope = await _call(runtime, step.video_tool)
        blocks = envelope.get('content') if isinstance(envelope, dict) else None
        if not isinstance(blocks, list):
            raise OperationError('The authorized tool returned no video')
        videos = [block for block in blocks if isinstance(block, dict)
                  and (video := _embedded_video(block)) is not None and video[0] == 'video/mp4']
        if len(videos) != 1:
            raise OperationError('A visual completion requires exactly one inline MP4')
        text = [block for block in blocks if isinstance(block, dict) and block.get('type') == 'text']
        projected = await _model_visible_result({'content': [*text, *videos]}, video_frame_extractor=extractor)
        if not getattr(projected, 'images', None):
            raise OperationError('The authorized video could not be sampled')
        prompt += '\n\nAuthorized video evidence:\n' + projected.content
        images = tuple(InferenceImage(image.content, image.mime_type) for image in projected.images)
    if len(prompt.encode()) > _MAX_PROMPT_BYTES:
        raise OperationError('Operation evidence exceeds the prompt bound')
    instructions = ('Return exactly one JSON value conforming to the following JSON Schema. '
                    'Use no markdown fences or explanatory text.\n' + canonical_json(schema))
    retry_instructions = instructions + '\nCheck that the answer is a single JSON value and meets every schema constraint.'
    messages = ({'role': 'user', 'content': prompt},)
    validator = Draft202012Validator(schema)
    for attempt in range(1, step.max_attempts + 1):
        request = InferenceRequest(messages, instructions if attempt == 1 else retry_instructions, images)
        response = await inference.complete(request)
        try:
            return _structured_output(response, validator)
        except OperationError:
            if attempt == step.max_attempts:
                raise


async def _execute_step(runtime, inference, step):
    async with asyncio.timeout(step.timeout_seconds):
        if isinstance(step, ToolStep):
            return await _call(runtime, step)
        if isinstance(step, CompletionStep):
            return await _complete(runtime, inference, step)
        for _ in range(_MAX_POLLS):
            result = await _call(runtime, step)
            decision = step.until(copy.deepcopy(result))
            if type(decision) is not bool:
                raise OperationError('A wait predicate must return a boolean')
            if decision:
                return result
            await asyncio.sleep(step.interval_seconds)
        raise OperationError('The wait reached its poll limit')


async def resolve_operation(selector, request: RunRequest, context: ExecutionContext) -> OperationInvocation | None:
    invocation = selector(request, context)
    if inspect.isawaitable(invocation):
        invocation = await invocation
    if invocation is not None and not isinstance(invocation, OperationInvocation):
        raise TypeError('The host selector must return an OperationInvocation')
    return invocation


async def _resume(runtime, store, request, context, operation, invocation, selector):
    chain, seen = [], {request.run_id}
    previous = invocation.resume_run_id
    while previous is not None:
        if not isinstance(previous, str) or not previous or previous in seen or len(chain) >= _MAX_RESUMES:
            raise OperationError('Invalid or exhausted operation checkpoint ancestry')
        seen.add(previous)
        old = await runtime.get_run(previous, context)
        if old.session_id != request.session_id or old.tenant_id != context.tenant_id or old.status not in _RESUMABLE:
            raise PermissionError('Operation continuation requires an interrupted or failed run in this session')
        accepted = await runtime.accepted_request(previous, context)
        fields = ('author', 'initiator', 'authority', 'audience', 'scopes', 'delegation_id', 'deferred', 'parent_run_id')
        if any(getattr(accepted, field) != getattr(context, field) for field in fields):
            raise PermissionError('Operation checkpoint belongs to another execution authority')
        events = await store.operation_events(previous)
        if not events or events[0].kind != STARTED:
            raise OperationError('No staged operation checkpoint was found')
        start = events[0].payload
        if start.get('name') != operation.name or start.get('version') != operation.version:
            raise OperationError('Operation checkpoint definition has changed')
        chain.append(events)
        previous = start.get('resume_run_id')

    origin_run_id = chain[-1][0].run_id
    original = await resolve_operation(selector, accepted.request, context)
    if original is None or original.name != operation.name or original.resume_run_id is not None:
        raise OperationError('The original accepted operation is unavailable')
    inputs = _input_object(original.inputs)
    digest = _digest(inputs)
    if invocation.inputs is not None and _digest(_input_object(invocation.inputs)) != digest:
        raise OperationError('Operation continuation inputs have changed')
    started, results, result_run_ids = {}, {}, {}
    for events in reversed(chain):
        metadata = events[0].payload
        if metadata.get('input_digest') != digest or metadata.get('origin_run_id') != origin_run_id:
            raise OperationError('Operation checkpoint does not match its original input')
        for event in events[1:]:
            key, step_digest = event.payload['key'], event.payload['step_digest']
            if key in results or key in started and started[key] != step_digest:
                raise OperationError('Operation checkpoint history is inconsistent')
            if event.kind == STEP_STARTED:
                started[key] = step_digest
            elif event.kind == STEP_COMPLETED and key in started:
                results[key] = event.payload['output']
                result_run_ids[key] = event.run_id
            else:
                raise OperationError('Operation checkpoint is missing its intent')
    return origin_run_id, inputs, started, results, result_run_ids


def _input_object(value) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError('Operation inputs must be a JSON object')
    return json.loads(_encode(dict(value)))


async def run_operation(runtime, request, context, operation, invocation, selector, inference) -> str:
    """Run the selected, versioned definition inside its admitted chat run."""
    from .runtime import current_execution_context, current_run_id, current_runtime

    if current_runtime() is not runtime or current_execution_context() != context or current_run_id() != request.run_id:
        raise PermissionError('A staged operation requires its exact admitted run')
    store = runtime.services.store
    if not callable(getattr(store, 'start_operation', None)) or not callable(getattr(store, 'operation_events', None)):
        raise OperationError('The runtime store does not support operation checkpoints')
    if invocation.resume_run_id is None:
        origin_run_id, inputs = request.run_id, _input_object(invocation.inputs)
        started, results, result_run_ids = {}, {}, {}
    else:
        origin_run_id, inputs, started, results, result_run_ids = await _resume(
            runtime, store, request, context, operation, invocation, selector,
        )
    result_bytes = sum(len(_encode(value)) for value in results.values())
    session = ResourceRef('session', context.tenant_id, context.session_id)
    await runtime.authorize(context, 'run.execute', session, audience=context.audience)
    await store.start_operation(request.run_id, {
        'name': operation.name, 'version': operation.version, 'input_digest': _digest(inputs),
        'origin_run_id': origin_run_id, 'resume_run_id': invocation.resume_run_id,
    })
    while True:
        record = await store.get(request.run_id)
        if record.cancel_requested:
            raise asyncio.CancelledError
        state = OperationState(origin_run_id, request.run_id, request.session_id,
                               MappingProxyType(copy.deepcopy(inputs)), MappingProxyType(copy.deepcopy(results)),
                               MappingProxyType(dict(result_run_ids)))
        step = operation.next_step(state)
        if isinstance(step, CompleteOperation):
            if not isinstance(step.output, str) or not step.output.strip() or len(step.output.encode()) > _MAX_VALUE_BYTES:
                raise OperationError('The operation did not produce a bounded final answer')
            return step.output
        if not isinstance(step, (ToolStep, WaitStep, CompletionStep)):
            raise TypeError('The operation must select a declared stage')
        if step.key in results or len(results) >= _MAX_STEPS:
            raise OperationError('Operation stage was repeated or the step limit was reached')
        digest = _step_digest(step)
        if step.key in started and (started[step.key] != digest or not _safe_to_retry(step)):
            raise OperationError('A previous operation stage has an uncertain effect; reconcile it before retrying')
        await runtime.authorize(context, 'run.execute', session, audience=context.audience)
        stamp = {'key': step.key, 'step_digest': digest}
        await store.append_event(request.run_id, STEP_STARTED, stamp)
        output = await _execute_step(runtime, inference, step)
        encoded = _encode(output)
        result_bytes += len(encoded)
        if result_bytes > _MAX_RESULTS_BYTES:
            raise OperationError('Operation checkpoints exceed their total byte limit')
        await runtime.authorize(context, 'run.publish', session, audience=context.audience)
        results[step.key] = json.loads(encoded)
        result_run_ids[step.key] = request.run_id
        await store.append_event(request.run_id, STEP_COMPLETED, {**stamp, 'output': results[step.key]})
