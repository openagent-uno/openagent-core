"""Fallback model configuration and helpers."""

from __future__ import annotations

from openagent_core.configuration import runtime_environment
import asyncio
import os
import threading
import time
from copy import copy, deepcopy
from contextlib import aclosing
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable, Iterator, List, Optional, Union

from openagent_core.core.runtime_errors import ContextWindowExceededError, ModelAuthenticationError, ModelProviderError, ModelRateLimitError
from openagent_core.models.credential_pool import retry_after_seconds, rotates_account
from openagent_core.models.providers.base import Model
from openagent_core.models.providers.response import ModelResponse, ModelResponseEvent
from openagent_core.core._run_state.agent import RunOutputEvent
from openagent_core.core._run_state.team import TeamRunOutputEvent
from openagent_core.core._runner.utils.log import log_warning

# Stream event type returned by response_stream / aresponse_stream
StreamEvent = Union[ModelResponse, RunOutputEvent, TeamRunOutputEvent]


@dataclass
class FallbackConfig:
    """Configuration for fallback model behavior.

    Example::

        FallbackConfig(
            on_error=[Claude(id="claude-sonnet-4-20250514")],
            on_rate_limit=[OpenAIChat(id="gpt-4o-mini")],
            on_context_overflow=[Claude(id="claude-sonnet-4-20250514")],
        )
    """

    # General fallback models tried when the primary model fails
    on_error: List[Union[Model, str]] = field(default_factory=list)
    # Fallback models tried specifically on rate-limit (429) errors
    on_rate_limit: List[Union[Model, str]] = field(default_factory=list)
    # Fallback models tried specifically on context-window-exceeded errors
    on_context_overflow: List[Union[Model, str]] = field(default_factory=list)
    # Optional callback invoked when a fallback model is activated.
    # Signature: callback(primary_model_id: str, fallback_model_id: str, error: Exception) -> None
    callback: Optional[Callable[[str, str, Exception], None]] = None
    # Automatic catalog recovery is distinct from an explicit/manual chain.
    recover_unavailable: bool = False
    eligible: Optional[Callable[[Model, dict], bool]] = None
    first_output_timeout: Optional[float] = None
    prepare: Optional[Callable[[], Any]] = None
    authorize_model: Optional[Callable[[Model], Any]] = None

    @property
    def has_fallbacks(self) -> bool:
        return bool(self.on_error or self.on_rate_limit or self.on_context_overflow)

    def resolve_models(self) -> None:
        """Resolve string model references to Model instances across all fallback lists.

        Deep copies model instances to avoid mutating shared objects when
        this FallbackConfig is reused across multiple agents.
        """
        from copy import deepcopy

        from openagent_core.core.metrics import ModelType
        from openagent_core.models.providers.utils import get_model

        for attr in ("on_error", "on_rate_limit", "on_context_overflow"):
            raw_list = getattr(self, attr)
            if raw_list:
                resolved: list = []
                for fm in raw_list:
                    # Una voce irrisolvibile si SALTA, non uccide l'agent.
                    # Era l'intento dichiarato ("a bad config degrades to 'no
                    # fallback' rather than crashing the agent") ma non era
                    # implementato per il caso che conta: `get_model()` torna
                    # None quando il modello non c'e', ma SOLLEVA ValueError su
                    # un provider che non e' un vendor nativo — e "codex:",
                    # "local:", "windows-local:" sono provider operatore con
                    # base_url nel DB, non vendor. Il 4-set-2026 sono morti
                    # cosi' sei run (alert-triage, morning-briefing-delegated),
                    # per giunta registrati come `success` con il ValueError al
                    # posto del risultato.
                    try:
                        resolved_model = get_model(fm)
                    except Exception as exc:  # noqa: BLE001
                        log_warning(
                            "fallback: voce non risolvibile, la salto: "
                            f"{fm!r} ({type(exc).__name__}: {exc})"
                        )
                        continue
                    if resolved_model is not None:
                        resolved_model = deepcopy(resolved_model)
                        resolved_model.model_type = ModelType.MODEL
                        resolved.append(resolved_model)
                setattr(self, attr, resolved)


# ---------------------------------------------------------------------------
# Fallback model selection
# ---------------------------------------------------------------------------


def get_fallback_models(fallback_config: Optional[FallbackConfig], error: Exception) -> Optional[List[Model]]:
    """Return the appropriate fallback list for the given error.

    Priority:
    1. Error-specific fallbacks (on_rate_limit / on_context_overflow)
    2. General fallback models (on_error) — only for retryable (5xx/network) errors
    3. Non-retryable client errors (400/401/403/etc.) are never masked by on_error
    """
    if fallback_config is None:
        return None
    # A host may replace refs after runner initialization. Resolve a private
    # snapshot so one unsupported ref never masks the actual provider failure
    # with AttributeError, nor mutates another in-flight runner's fallback list.
    if any(isinstance(model, str) for attr in
           ("on_error", "on_rate_limit", "on_context_overflow")
           for model in getattr(fallback_config, attr)):
        fallback_config = copy(fallback_config)
        fallback_config.resolve_models()

    if fallback_config.recover_unavailable:
        from openagent_core.models.automatic_fallback import unavailable_model_error
        if unavailable_model_error(error):
            return fallback_config.on_error or None

    if isinstance(error, ModelRateLimitError) and fallback_config.on_rate_limit:
        return fallback_config.on_rate_limit  # type: ignore[return-value]
    if isinstance(error, ContextWindowExceededError) and fallback_config.on_context_overflow:
        return fallback_config.on_context_overflow  # type: ignore[return-value]
    # For any ModelProviderError that wasn't already classified, try to classify it
    if isinstance(error, ModelProviderError):
        classified = ModelProviderError.classify(error)
        if isinstance(classified, ModelRateLimitError) and fallback_config.on_rate_limit:
            return fallback_config.on_rate_limit  # type: ignore[return-value]
        if isinstance(classified, ContextWindowExceededError) and fallback_config.on_context_overflow:
            return fallback_config.on_context_overflow  # type: ignore[return-value]
    # Don't mask non-retryable client errors (401/403/etc.) — these are
    # configuration bugs that the developer needs to see and fix.
    # Rate-limit (429/529) and context-window errors are excluded since they
    # are legitimate fallback scenarios even when their specific lists are empty.
    _retryable_status_codes = {429, 529}
    if (
        isinstance(error, ModelProviderError)
        and not isinstance(error, (ModelRateLimitError, ContextWindowExceededError))
        and error.status_code
        and 400 <= error.status_code < 500
        and error.status_code not in _retryable_status_codes
    ):
        return None
    return fallback_config.on_error or None  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Sync / async model calls with fallback
# ---------------------------------------------------------------------------


def _check_automatic_primary(model, config, kwargs):
    if config is None or not config.recover_unavailable:
        return
    cooling = _breaker_enabled() and _breaker_quarantined(_health_id(model), time.monotonic())
    if ((config.eligible is not None and not config.eligible(model, kwargs)) or cooling):
        from openagent_core.core.public_errors import PUBLIC_MESSAGES
        with _breaker_lock:
            code = _breaker_public_codes.get(_health_id(model), "model_unavailable") if cooling else "model_unavailable"
        error = ModelProviderError(PUBLIC_MESSAGES[code], status_code=503,
                                   model_id=_route_id(model))
        error.routing_skip = True
        raise error
    # Auto owns the bounded retry/fallback policy; SDK-local retries would
    # repeat an exhausted account before the next eligible connection.
    if hasattr(model, 'max_retries'):
        model.max_retries = 0


async def _initial_response(model, config, kwargs):
    if config is not None and config.prepare is not None:
        await config.prepare()
    _check_automatic_primary(model, config, kwargs)
    await _authorize_automatic_model(model, config)
    timeout = config.first_output_timeout if config is not None and config.recover_unavailable else None
    try:
        result = await asyncio.wait_for(model.aresponse(**kwargs), timeout) if timeout else await model.aresponse(**kwargs)
        from openagent_core.models.account_routes import record_model_result
        record_model_result(_route_id(model), model, result)
        return result
    except asyncio.TimeoutError:
        raise ModelProviderError("The selected model did not answer within its routing deadline",
                                 status_code=504, model_id=_route_id(model)) from None
    except ModelAuthenticationError as error:
        if config is None or not config.recover_unavailable:
            raise
        raise ModelProviderError(str(error), status_code=401, model_id=_route_id(model)) from error


async def _initial_stream(model, config, kwargs):
    if config is not None and config.prepare is not None:
        await config.prepare()
    _check_automatic_primary(model, config, kwargs)
    await _authorize_automatic_model(model, config)
    iterator = (_abounded_first_output(model, kwargs, config.first_output_timeout)
                if config is not None and config.recover_unavailable else model.aresponse_stream(**kwargs))
    try:
        async for event in iterator:
            if not _is_local_marker(event):
                from openagent_core.models.account_routes import record_model_result
                record_model_result(_route_id(model), model, event, streaming=True)
            yield event
    except ModelAuthenticationError as error:
        if config is None or not config.recover_unavailable:
            raise
        raise ModelProviderError(str(error), status_code=401, model_id=_route_id(model)) from error
    finally:
        await iterator.aclose()


def _record_automatic_failure(model, config, error):
    if (config is not None and config.recover_unavailable and not getattr(error, "routing_skip", False)
            and get_fallback_models(config, error)):
        _breaker_record_failure(_health_id(model), error)


async def _authorize_automatic_model(model, config):
    if config is None or config.authorize_model is None:
        return
    try:
        await config.authorize_model(model)
    except PermissionError:
        error = ModelProviderError("The selected model is not authorized for this request",
                                   status_code=503, model_id=_route_id(model))
        error.routing_skip = True
        raise error from None


def _clean_kwargs_for_fallback(kwargs: dict) -> dict:
    """Return a copy of kwargs with a fresh messages list.

    The primary model's retry-with-guidance logic may have appended
    provider-specific guidance messages to kwargs["messages"]. Fallback
    models should not see those, so we strip any temporary messages.
    """
    cleaned = dict(kwargs)
    if "messages" in cleaned:
        cleaned["messages"] = [m for m in cleaned["messages"] if not getattr(m, "temporary", False)]
    return cleaned


# ---------------------------------------------------------------------------
# In-place deterministic recovery (additive; runs on the model-error HOT PATH,
# BEFORE credential rotation / get_fallback_models). For a matched, recoverable
# error signature we apply a PURE transform to a *copy* of the request and retry
# the SAME model once. If nothing matches — or the repair/retry fails — the
# original request is untouched and control falls through to the exact existing
# fallback path (byte-identical to today). Every branch below repairs a case
# that hard-fails today: a 400 that OpenAgent classifies as a non-retryable
# client error and raises straight up (proven in recon). The whole block is
# defensively wrapped by callers so any bug here degrades to today's fallback.
# ---------------------------------------------------------------------------

# Kill-switch: default ON — each branch is a strict improvement over a turn
# that otherwise fails outright. Set OPENAGENT_INPLACE_RECOVERY_ENABLED to a
# falsy value (0/false/no/off) to disable the whole feature redeploy-free.
_INPLACE_RECOVERY_ENV = "OPENAGENT_INPLACE_RECOVERY_ENABLED"

# Anthropic per-image ceiling & generic image-rejection wordings (narrow on
# purpose — each is a verbatim provider phrase, not a generic word like "large").
_IMAGE_TOO_LARGE_PATTERNS = (
    "image exceeds",
    "image too large",
    "image_too_large",
    "image size exceeds",
    "image dimensions exceed",
    "dimensions exceed max allowed size",
)
# HTTP-413-style payload rejections. Only actioned when the request actually
# carries images (else it is a text-size overflow that the existing
# context-overflow path already handles — see get_fallback_models).
_PAYLOAD_TOO_LARGE_PATTERNS = (
    "request entity too large",
    "payload too large",
    "error code: 413",
)


def _inplace_recovery_enabled() -> bool:
    return runtime_environment().get(_INPLACE_RECOVERY_ENV, "1").strip().lower() not in {"0", "false", "no", "off"}


def _classify_inplace_recovery(error: Exception) -> Optional[str]:
    """Return a recovery-kind tag for a deterministically-repairable error, else None.

    Signatures are matched on (status_code + verbatim message pattern) and are
    intentionally narrow so an unrelated error is NEVER captured — that is what
    keeps the no-match path byte-identical to today.
    """
    status = getattr(error, "status_code", None)
    msg = str(getattr(error, "message", "") or error).lower()

    # Anthropic thinking-block signature invalidated by upstream history
    # mutation (compaction / truncation / merge) → 400 "signature" + "thinking".
    if status == 400 and "signature" in msg and "thinking" in msg:
        return "thinking_signature"

    # Strict json-schema-to-grammar backends (e.g. llama.cpp's OAI server)
    # reject regex escapes in tool-schema `pattern` / `format` → 400.
    # ``failed to parse grammar`` is the wording llama.cpp actually returns
    # today, inside "Failed to initialize samplers: failed to parse grammar":
    # measured 24-ago-2026 against the self-hosted Qwen row, where a tool schema
    # carrying an e-mail ``pattern`` made EVERY tool call 400 while the identical
    # schema without it answered fine. The three signatures below did not match
    # it, so the repair that exists for exactly this case never fired and the
    # turn died — the one thing that broke when switching to that model.
    if status == 400 and (
        "error parsing grammar" in msg
        or "failed to parse grammar" in msg
        or "json-schema-to-grammar" in msg
        or ("unable to generate parser" in msg and "template" in msg)
    ):
        return "tool_schema_pattern"

    # Oversized image / payload-too-large → strip images, retry text-only.
    if status in (400, 413):
        if any(p in msg for p in _IMAGE_TOO_LARGE_PATTERNS):
            return "image_too_large"
        if any(p in msg for p in _PAYLOAD_TOO_LARGE_PATTERNS):
            return "payload_too_large"
    return None


def _is_image_content_part(part: Any) -> bool:
    """True for a structured content part that carries an image."""
    return isinstance(part, dict) and part.get("type") in ("image", "image_url", "input_image")


def _strip_images_from_kwargs(kwargs: dict) -> Optional[dict]:
    """Return a repaired kwargs copy with image parts removed, or None if none found.

    Clears ``Message.images`` and drops image content-parts from list-shaped
    content. Builds fresh Message copies via ``model_copy`` — the caller's
    original message objects are never mutated.
    """
    messages = kwargs.get("messages")
    if not messages:
        return None
    changed = False
    new_messages: list = []
    for m in messages:
        update: dict = {}
        if getattr(m, "images", None):
            update["images"] = None
        content = getattr(m, "content", None)
        if isinstance(content, list):
            filtered = [p for p in content if not _is_image_content_part(p)]
            if len(filtered) != len(content):
                update["content"] = filtered
        if update:
            new_messages.append(m.model_copy(update=update))
            changed = True
        else:
            new_messages.append(m)
    if not changed:
        return None
    repaired = dict(kwargs)
    repaired["messages"] = new_messages
    return repaired


def _strip_thinking_from_kwargs(kwargs: dict) -> Optional[dict]:
    """Return a repaired kwargs copy with reasoning/thinking blocks stripped.

    Anthropic emits a thinking block for an assistant message only when
    ``reasoning_content`` and ``provider_data`` are both set (see
    ``format_messages``); clearing ``reasoning_content`` /
    ``redacted_reasoning_content`` and dropping the ``signature`` from
    ``provider_data`` is enough to send no signed thinking blocks on retry.
    """
    messages = kwargs.get("messages")
    if not messages:
        return None
    changed = False
    new_messages: list = []
    for m in messages:
        update: dict = {}
        if getattr(m, "reasoning_content", None) is not None:
            update["reasoning_content"] = None
        if getattr(m, "redacted_reasoning_content", None) is not None:
            update["redacted_reasoning_content"] = None
        pd = getattr(m, "provider_data", None)
        if isinstance(pd, dict) and "signature" in pd:
            update["provider_data"] = {k: v for k, v in pd.items() if k != "signature"}
        if update:
            new_messages.append(m.model_copy(update=update))
            changed = True
        else:
            new_messages.append(m)
    if not changed:
        return None
    repaired = dict(kwargs)
    repaired["messages"] = new_messages
    return repaired


def _strip_schema_keys(node: Any, keys: tuple) -> int:
    """Recursively drop ``keys`` from a JSON-schema tree / Function.parameters.

    Returns the number of keys removed. Walks dicts, lists, and any object that
    exposes a ``parameters`` attribute (a Function tool).
    """
    removed = 0
    if isinstance(node, dict):
        for k in keys:
            if k in node:
                node.pop(k, None)
                removed += 1
        for v in list(node.values()):
            removed += _strip_schema_keys(v, keys)
    elif isinstance(node, list):
        for item in node:
            removed += _strip_schema_keys(item, keys)
    else:
        params = getattr(node, "parameters", None)
        if params is not None:
            removed += _strip_schema_keys(params, keys)
    return removed


def _strip_tool_schema_patterns_from_kwargs(kwargs: dict) -> Optional[dict]:
    """Return a repaired kwargs copy with ``pattern``/``format`` stripped from tools.

    Tools are deep-copied first so the caller's shared Function/dict objects are
    never mutated. Returns None if nothing was stripped.
    """
    tools = kwargs.get("tools")
    if not tools:
        return None
    new_tools = deepcopy(tools)
    if _strip_schema_keys(new_tools, ("pattern", "format")) == 0:
        return None
    repaired = dict(kwargs)
    repaired["tools"] = new_tools
    return repaired


def _repair_kwargs_for_recovery(kind: str, kwargs: dict) -> Optional[dict]:
    """Dispatch a recovery kind to its pure request transform.

    Returns a repaired shallow-copy of kwargs (originals untouched), or None if
    the fix does not actually apply to this request (→ caller falls through).
    """
    if kind in ("image_too_large", "payload_too_large"):
        return _strip_images_from_kwargs(kwargs)
    if kind == "thinking_signature":
        return _strip_thinking_from_kwargs(kwargs)
    if kind == "tool_schema_pattern":
        return _strip_tool_schema_patterns_from_kwargs(kwargs)
    return None


def _plan_inplace_recovery(error: Exception, kwargs: dict) -> Optional[tuple]:
    """Best-effort: return (kind, repaired_kwargs) if this error is repairable.

    Fully self-contained and exception-safe: any failure inside classification
    or transform is swallowed and reported as "no recovery", so the model-error
    hot path can never be made worse than today by a bug in here.
    """
    try:
        if not _inplace_recovery_enabled():
            return None
        kind = _classify_inplace_recovery(error)
        if kind is None:
            return None
        repaired = _repair_kwargs_for_recovery(kind, kwargs)
        if repaired is None:
            return None
        return kind, repaired
    except Exception:
        return None


def call_model_with_fallback(
    model: Model,
    fallback_config: Optional[FallbackConfig],
    **kwargs: Any,
) -> ModelResponse:
    """Call the primary model, falling back on failure.

    Each model (including primary) uses its own retry logic before moving to the next.
    """
    try:
        return model.response(**kwargs)
    except ModelProviderError as primary_error:
        _record_automatic_failure(model, fallback_config, primary_error)
        fallbacks = get_fallback_models(fallback_config, primary_error)
        if not fallbacks:
            raise
        log_warning(f"Primary model '{model.id}' failed. Trying fallback models...: {primary_error}")
        return _try_fallback_models(
            fallbacks, primary_error, "response", _clean_kwargs_for_fallback(kwargs), _route_id(model), fallback_config
        )


async def acall_model_with_fallback(
    model: Model,
    fallback_config: Optional[FallbackConfig],
    **kwargs: Any,
) -> ModelResponse:
    """Async variant of call_model_with_fallback."""
    initial_messages = len(kwargs.get('messages') or [])
    try:
        return await _initial_response(model, fallback_config, kwargs)
    except ModelProviderError as primary_error:
        if _committed_tools(kwargs, initial_messages):
            raise
        # ── In-place deterministic recovery (additive, best-effort) ──
        # Before rotating credentials or falling over to another provider,
        # try a deterministic repair of the request and retry the SAME model
        # once. Only matched, recoverable signatures are touched; the original
        # `kwargs` is never mutated, so if this yields nothing (no match) or the
        # repaired retry fails, control drops to the exact existing path below
        # with a byte-identical request. Fully wrapped so any bug degrades to
        # today's fallback contract, never propagates.
        try:
            plan = _plan_inplace_recovery(primary_error, kwargs)
            if plan is not None:
                kind, repaired = plan
                log_warning(
                    f"In-place recovery '{kind}' for model '{model.id}': "
                    f"repaired request and retrying same model once."
                )
                return await model.aresponse(**repaired)
        except ModelProviderError:
            pass  # repaired retry still failed → existing fallback path below
        except Exception:
            pass  # any recovery bug → existing fallback path below

        # ── Credential-pool rotation (inert unless the model carries a pool) ──
        # On a rate-limit, rotate across this provider's other accounts BEFORE
        # consulting get_fallback_models. Best-effort: any exception inside the
        # pool block degrades to the existing fallback path, never propagates.
        pool = getattr(model, "_openagent_cred_pool", None)
        if pool is not None:
            try:
                while rotates_account(primary_error):
                    if _committed_tools(kwargs, initial_messages):
                        break
                    nxt = pool.mark_exhausted_and_rotate(
                        status_code=ModelProviderError.classify(primary_error).status_code,
                        api_key_hint=model.api_key,
                        retry_after_s=retry_after_seconds(primary_error),
                    )
                    if nxt is None:
                        break  # pool drained → existing fallback path below
                    model.api_key, model.base_url = nxt.api_key, nxt.base_url
                    model.client = model.async_client = None
                    try:
                        return await _initial_response(model, fallback_config, kwargs)
                    except ModelProviderError as e:
                        # The next account's failure decides: another limit or
                        # rejected credential keeps rotating, anything else is
                        # the more actionable error and is surfaced.
                        primary_error = e
            except Exception:
                pass  # never let a pool bug break the fallback contract

        if _committed_tools(kwargs, initial_messages):
            raise primary_error
        _record_automatic_failure(model, fallback_config, primary_error)
        fallbacks = get_fallback_models(fallback_config, primary_error)
        if not fallbacks:
            raise primary_error
        log_warning(f"Primary model '{model.id}' failed. Trying fallback models...: {primary_error}")
        return await _atry_fallback_models(
            fallbacks, primary_error, "aresponse", _clean_kwargs_for_fallback(kwargs), _route_id(model), fallback_config
        )


# ---------------------------------------------------------------------------
# Sync / async stream calls with fallback
# ---------------------------------------------------------------------------


def call_model_stream_with_fallback(
    model: Model,
    fallback_config: Optional[FallbackConfig],
    **kwargs: Any,
) -> Iterator[StreamEvent]:
    """Call the primary model stream, falling back on failure."""
    emitted = False
    try:
        for event in model.response_stream(**kwargs):
            emitted = emitted or not _is_local_marker(event)
            yield event
    except ModelProviderError as primary_error:
        if emitted:
            raise
        _record_automatic_failure(model, fallback_config, primary_error)
        fallbacks = get_fallback_models(fallback_config, primary_error)
        if not fallbacks:
            raise
        log_warning(f"Primary model '{model.id}' failed. Trying fallback models...: {primary_error}")
        yield ModelResponse(event=ModelResponseEvent.fallback_model_activated.value)
        yield from _try_fallback_models_stream(
            fallbacks, primary_error, _clean_kwargs_for_fallback(kwargs), _route_id(model), fallback_config
        )


# One quick retry on the same model for a transient failure that produced no
# output: a dropped connection, a 5xx, or a stream that errored before its first
# event. Without it a single network blip ends a long agent turn, because the
# default model has ``retries = 0`` and many agents configure no fallback.
TRANSIENT_STREAM_RETRIES = 1
TRANSIENT_STREAM_RETRY_DELAY_S = 1.0


def _is_transient_stream_error(error: ModelProviderError) -> bool:
    if getattr(error, "routing_skip", False):
        return False
    try:
        classified = ModelProviderError.classify(error)
    except Exception:
        return False
    if isinstance(classified, (ModelRateLimitError, ContextWindowExceededError)):
        return False
    status = getattr(classified, "status_code", None)
    return status is None or (isinstance(status, int) and status >= 500)


async def acall_model_stream_with_fallback(
    model: Model,
    fallback_config: Optional[FallbackConfig],
    **kwargs: Any,
) -> AsyncIterator[StreamEvent]:
    """Async variant of call_model_stream_with_fallback."""
    any_event_emitted = False
    try:
        async with aclosing(_initial_stream(model, fallback_config, kwargs)) as stream:
            async for event in stream:
                any_event_emitted = any_event_emitted or not _is_local_marker(event)
                yield event
        return
    except ModelProviderError as e:
        primary_error: ModelProviderError = e

    # ── Transient zero-output retry on the same model ──
    # Only before the first event, so nothing is ever duplicated. A retry that
    # emits output and then fails propagates like any mid-stream failure.
    retries_left = TRANSIENT_STREAM_RETRIES
    while not any_event_emitted and retries_left > 0 and _is_transient_stream_error(primary_error):
        retries_left -= 1
        log_warning(
            f"Transient stream failure on model '{model.id}' before any output; "
            f"retrying once: {primary_error}"
        )
        await asyncio.sleep(TRANSIENT_STREAM_RETRY_DELAY_S)
        try:
            async with aclosing(_initial_stream(model, fallback_config, kwargs)) as stream:
                async for event in stream:
                    any_event_emitted = any_event_emitted or not _is_local_marker(event)
                    yield event
            return
        except ModelProviderError as e:
            if any_event_emitted:
                raise
            primary_error = e

    # ── Credential-pool rotation (inert unless the model carries a pool) ──
    # Only safe BEFORE the first event: a mid-stream failure has already
    # emitted output, so rotating would duplicate it — those cases fall
    # straight through to the existing fallback path. Best-effort throughout;
    # any pool bug degrades to that same path.
    # ── In-place deterministic recovery (stream, additive, best-effort) ──
    # Safe ONLY before the first event (a mid-stream failure has already emitted
    # output). The repaired attempt is BUFFERED and only flushed once it streams
    # to completion — so if it fails at any point we discard the buffer and drop
    # to the exact existing path with zero committed output (byte-identical).
    if not any_event_emitted:
        plan = _plan_inplace_recovery(primary_error, kwargs)
        if plan is not None:
            kind, repaired = plan
            buffered: list = []
            recovered = False
            try:
                async with aclosing(model.aresponse_stream(**repaired)) as stream:
                    async for event in stream:
                        buffered.append(event)
                recovered = True
            except Exception:
                recovered = False  # discard buffer → existing path unchanged
            if recovered:
                log_warning(
                    f"In-place recovery '{kind}' for model '{model.id}': "
                    f"repaired request streamed cleanly on the same model."
                )
                for event in buffered:
                    yield event
                return

    if not any_event_emitted:
        pool = getattr(model, "_openagent_cred_pool", None)
        while pool is not None and rotates_account(primary_error):
            try:
                nxt = pool.mark_exhausted_and_rotate(
                    status_code=ModelProviderError.classify(primary_error).status_code,
                    api_key_hint=model.api_key,
                    retry_after_s=retry_after_seconds(primary_error),
                )
            except Exception:
                break
            if nxt is None:
                break  # pool drained → existing fallback path below
            model.api_key, model.base_url = nxt.api_key, nxt.base_url
            model.client = model.async_client = None
            attempt_emitted = False
            try:
                async with aclosing(_initial_stream(model, fallback_config, kwargs)) as stream:
                    async for event in stream:
                        attempt_emitted = attempt_emitted or not _is_local_marker(event)
                        any_event_emitted = any_event_emitted or attempt_emitted
                        yield event
                return  # rotated account streamed cleanly
            except ModelProviderError as e:
                primary_error = e
                if attempt_emitted:
                    # Committed output on this account then failed — cannot
                    # rotate again without duplicating; degrade to fallback.
                    break
                # zero-event limit or rejected credential → try the next one
            except Exception:
                break  # non-provider stream error → degrade to fallback

    # Never replay provider output or tool calls on another model.
    if any_event_emitted:
        raise primary_error
    _record_automatic_failure(model, fallback_config, primary_error)
    fallbacks = get_fallback_models(fallback_config, primary_error)
    if not fallbacks:
        raise primary_error
    log_warning(f"Primary model '{model.id}' failed. Trying fallback models...: {primary_error}")
    yield ModelResponse(event=ModelResponseEvent.fallback_model_activated.value)
    async with aclosing(_atry_fallback_models_stream(
        fallbacks, primary_error, _clean_kwargs_for_fallback(kwargs), _route_id(model), fallback_config
    )) as stream:
        async for event in stream:
            yield event


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _notify_fallback(
    fallback_config: Optional[FallbackConfig],
    primary_model_id: str,
    fallback_model_id: str,
    error: Exception,
) -> None:
    """Invoke the on_fallback callback if configured."""
    if fallback_config and fallback_config.callback:
        try:
            fallback_config.callback(primary_model_id, fallback_model_id, error)
        except Exception:
            pass  # Don't let callback errors break fallback flow


# ---------------------------------------------------------------------------
# Breaker half-open sui gradini della catena
# ---------------------------------------------------------------------------
# La catena era una lista statica provata sempre nello stesso ordine. Due difetti
# misurati in produzione il 24-ago-2026:
#
#  1. nessuna memoria. Un proxy di abbonamento parcheggia un modello che ha preso il
#     muro (``mark_rate_limited(..., model=...)``); finche' dura, OGNI turno
#     pagava un viaggio a vuoto sullo stesso gradino prima di passare al
#     successivo. L'informazione c'era gia', la riscoprivamo ogni volta a spese
#     nostre.
#  2. la ripresa era ad orologio. Upstream ha risposto "riprova fra 486262
#     secondi" (5,6 giorni) e il proxy ha tenuto ``gpt-5.3-codex-spark`` fuori
#     per il suo tetto pieno di 3 ore, senza mai verificare se nel frattempo
#     fosse tornato.
#
# Qui il gradino che ha appena fallito viene SPOSTATO IN FONDO, non tolto: se
# tutti sono in quarantena l'ordine resta quello configurato e si prova
# comunque, perche' una lista vuota e' sempre peggio di un tentativo che forse
# fallisce (stessa disciplina del budget guard). Scaduta la quarantena il
# gradino torna in testa e il primo turno che passa e' la sonda: se risponde,
# lo stato sparisce all'istante; se no, la quarantena raddoppia fino al tetto.
# La ripresa e' quindi VERIFICATA da una richiesta vera, mai assunta da un
# timer, che e' la stessa regola che quota-guard applica ai provider di riserva.
#
# Lo stato vive in memoria e muore col processo: e' un acceleratore, non una
# fonte di verita'. Perderlo costa un viaggio a vuoto, non una risposta.
_BREAKER_BASE_SECONDS = 120.0
_BREAKER_MAX_SECONDS = 2700.0
_breaker_lock = threading.Lock()
# model id -> (istante in cui si puo' riprovare, fallimenti consecutivi)
from openagent_core.instance_state import InstanceMapping
_breaker = InstanceMapping('provider-fallback-breaker')
# The cooldown never retains provider exception objects or diagnostics.
_breaker_public_codes = InstanceMapping('provider-fallback-public-codes')


def _breaker_enabled() -> bool:
    return runtime_environment().get("OPENAGENT_FALLBACK_BREAKER_ENABLED", "1").strip().lower() not in (
        "0", "false", "no", "off",
    )


def _breaker_quarantined(model_id: str, now: float) -> bool:
    with _breaker_lock:
        entry = _breaker.get(model_id)
        return bool(entry and now < entry[0])


def _breaker_record_failure(model_id: str, error: BaseException | None = None) -> None:
    if not model_id:
        return
    from openagent_core.core.public_errors import classify_run_error
    code = classify_run_error(error).code if error is not None else "model_unavailable"
    with _breaker_lock:
        _, failures = _breaker.get(model_id, (0.0, 0))
        failures += 1
        env = runtime_environment()
        base = max(0.0, float(env.get('OPENAGENT_FALLBACK_BREAKER_SECONDS', _BREAKER_BASE_SECONDS)))
        ceiling = max(base, float(env.get('OPENAGENT_FALLBACK_BREAKER_MAX_SECONDS', _BREAKER_MAX_SECONDS)))
        delay = min(base * (2 ** min(failures - 1, 30)), ceiling)
        _breaker[model_id] = (time.monotonic() + delay, failures)
        _breaker_public_codes[model_id] = code
    log_warning(f"Fallback breaker: '{model_id}' in quarantena per {delay:.0f}s (fallimento #{failures})")


def _breaker_record_success(model_id: str) -> None:
    if not model_id:
        return
    with _breaker_lock:
        _breaker.pop(model_id, None)
        _breaker_public_codes.pop(model_id, None)


def breaker_snapshot() -> dict[str, float]:
    """Secondi di quarantena residui per modello. Per i test e la diagnosi."""
    now = time.monotonic()
    with _breaker_lock:
        return {m: max(0.0, until - now) for m, (until, _) in _breaker.items()}


def reset_breaker() -> None:
    """Svuota lo stato. Serve ai test, non al runtime."""
    with _breaker_lock:
        _breaker.clear()
        _breaker_public_codes.clear()


def _route_id(model):
    return str(getattr(model, "_openagent_runtime_id", None) or getattr(model, "id", ""))


def _health_id(model):
    scope = getattr(model, '_openagent_health_scope', None)
    if scope is None:
        return _route_id(model)
    import hashlib
    return hashlib.sha256(str(scope).encode()).hexdigest()[:24] + ':' + _route_id(model)


def _committed_tools(kwargs, initial_messages):
    for message in (kwargs.get('messages') or [])[initial_messages:]:
        value = message.get if isinstance(message, dict) else lambda key: getattr(message, key, None)
        if value('tool_calls') or value('role') == 'tool':
            return True
    return False


def _ordered_candidates(fallback_models: List[Model], *, fallback_config=None,
                        kwargs=None, primary_model_id="") -> List[Model]:
    """I candidati non in quarantena per primi, gli altri in coda.

    Riordina, non filtra: nessun gradino viene mai perso.
    """
    candidates = list(fallback_models)
    if fallback_config is not None and fallback_config.eligible is not None:
        candidates = [model for model in candidates if fallback_config.eligible(model, kwargs or {})]
    if fallback_config is not None and fallback_config.recover_unavailable:
        primary = next((model for model in candidates if _route_id(model) == primary_model_id), None)
        connection = getattr(primary, "_openagent_connection_key", None)
        candidates = [model for model in candidates if _route_id(model) != primary_model_id
                      and not _breaker_quarantined(_health_id(model), time.monotonic())]
        candidates.sort(key=lambda model: (
            bool(getattr(model, '_openagent_automatic_backup', False)),
            (getattr(model, "_openagent_connection_key", None) == connection if connection is not None
             else _route_id(model).split(":", 1)[0] == primary_model_id.split(":", 1)[0])
            if getattr(model, '_openagent_automatic_backup', False) else False))
    if not _breaker_enabled() or len(candidates) < 2:
        return candidates
    now = time.monotonic()
    ready = [m for m in candidates if not _breaker_quarantined(_health_id(m), now)]
    held = [m for m in candidates if _breaker_quarantined(_health_id(m), now)]
    return ready + held


def _try_fallback_models(
    fallback_models: List[Model],
    primary_error: Exception,
    method_name: str,
    kwargs: dict,
    primary_model_id: str = "",
    fallback_config: Optional[FallbackConfig] = None,
) -> ModelResponse:
    """Try each fallback model in order. Raises the primary error if all fail."""
    candidates = _ordered_candidates(fallback_models, fallback_config=fallback_config, kwargs=kwargs, primary_model_id=primary_model_id)
    for i, fallback in enumerate(candidates):
        try:
            log_warning(f"Trying fallback model {i + 1}/{len(candidates)}: {fallback.id}")
            result = getattr(fallback, method_name)(**kwargs)
            _breaker_record_success(_health_id(fallback))
            _notify_fallback(fallback_config, primary_model_id, fallback.id, primary_error)
            return result
        except ModelProviderError as e:
            _breaker_record_failure(_health_id(fallback), e)
            log_warning(f"Fallback model '{fallback.id}' also failed: {str(e)}")
            continue
    raise primary_error


async def _atry_fallback_models(
    fallback_models: List[Model],
    primary_error: Exception,
    method_name: str,
    kwargs: dict,
    primary_model_id: str = "",
    fallback_config: Optional[FallbackConfig] = None,
) -> ModelResponse:
    """Async: try each fallback model in order. Raises the primary error if all fail."""
    candidates = _ordered_candidates(fallback_models, fallback_config=fallback_config, kwargs=kwargs, primary_model_id=primary_model_id)
    for i, fallback in enumerate(candidates):
        initial_messages = len(kwargs.get('messages') or [])
        try:
            log_warning(f"Trying fallback model {i + 1}/{len(candidates)}: {fallback.id}")
            result = (await _initial_response(fallback, fallback_config, kwargs)
                      if method_name == "aresponse" else await getattr(fallback, method_name)(**kwargs))
            from openagent_core.models.account_routes import record_model_result
            # _initial_response already captured this model's reported usage.
            record_model_result(primary_model_id, fallback, ModelResponse())
            _breaker_record_success(_health_id(fallback))
            _notify_fallback(fallback_config, primary_model_id, fallback.id, primary_error)
            return result
        except ModelProviderError as e:
            if _committed_tools(kwargs, initial_messages):
                raise
            _breaker_record_failure(_health_id(fallback), e)
            log_warning(f"Fallback model '{fallback.id}' also failed: {str(e)}")
            continue
    raise primary_error


def _try_fallback_models_stream(
    fallback_models: List[Model],
    primary_error: Exception,
    kwargs: dict,
    primary_model_id: str = "",
    fallback_config: Optional[FallbackConfig] = None,
) -> Iterator[StreamEvent]:
    """Try each fallback model stream in order. Raises the primary error if all fail."""
    candidates = _ordered_candidates(fallback_models, fallback_config=fallback_config, kwargs=kwargs, primary_model_id=primary_model_id)
    for i, fallback in enumerate(candidates):
        emitted = False
        try:
            log_warning(f"Trying fallback model {i + 1}/{len(candidates)}: {fallback.id}")
            for event in fallback.response_stream(**kwargs):
                emitted = emitted or not _is_local_marker(event)
                yield event
            _breaker_record_success(_health_id(fallback))
            _notify_fallback(fallback_config, primary_model_id, fallback.id, primary_error)
            return
        except ModelProviderError as e:
            if emitted:
                raise
            _breaker_record_failure(_health_id(fallback), e)
            log_warning(f"Fallback model '{fallback.id}' also failed: {str(e)}")
            continue
    raise primary_error


# A backup reached through a path that accepts the connection and never answers
# (an offline machine behind a userspace VPN, a stopped local server) would hold
# the turn for the provider SDK's read timeout and its retries. Each backup gets
# a bounded wait for its first provider output; local markers emitted before
# the network round trip restart the wait, and once output has started the
# stream is never cut. Zero or a negative value disables the bound.
FALLBACK_FIRST_OUTPUT_TIMEOUT_ENV = "OPENAGENT_FALLBACK_FIRST_OUTPUT_TIMEOUT_S"
DEFAULT_FALLBACK_FIRST_OUTPUT_TIMEOUT_S = 120.0
_LOCAL_MARKER_EVENTS = frozenset({
    ModelResponseEvent.model_request_started.value,
    ModelResponseEvent.compression_started.value,
    ModelResponseEvent.compression_completed.value,
    ModelResponseEvent.fallback_model_activated.value,
})


def _fallback_first_output_timeout_s() -> Optional[float]:
    raw = str(runtime_environment().get(FALLBACK_FIRST_OUTPUT_TIMEOUT_ENV, "")).strip()
    try:
        value = float(raw) if raw else DEFAULT_FALLBACK_FIRST_OUTPUT_TIMEOUT_S
    except ValueError:
        value = DEFAULT_FALLBACK_FIRST_OUTPUT_TIMEOUT_S
    return value if value > 0 else None


def _is_local_marker(event: StreamEvent) -> bool:
    return isinstance(event, ModelResponse) and event.event in _LOCAL_MARKER_EVENTS


async def _abounded_first_output(model: Model, kwargs: dict, timeout_seconds=None) -> AsyncIterator[StreamEvent]:
    """Stream ``model``; fail if no provider output arrives within the bound."""
    timeout = timeout_seconds if timeout_seconds is not None else _fallback_first_output_timeout_s()
    iterator = model.aresponse_stream(**kwargs).__aiter__()
    waiting = timeout is not None
    try:
        while True:
            try:
                if waiting:
                    event = await asyncio.wait_for(iterator.__anext__(), timeout)
                else:
                    event = await iterator.__anext__()
            except StopAsyncIteration:
                return
            except asyncio.TimeoutError:
                raise ModelProviderError(
                    message=f"Model '{model.id}' produced no output within {timeout:g}s",
                    status_code=504,
                    model_name=getattr(model, "name", None),
                    model_id=str(model.id),
                ) from None
            if waiting and not _is_local_marker(event):
                waiting = False
            yield event
    finally:
        aclose = getattr(iterator, "aclose", None)
        if aclose is not None:
            await aclose()


async def _atry_fallback_models_stream(
    fallback_models: List[Model],
    primary_error: Exception,
    kwargs: dict,
    primary_model_id: str = "",
    fallback_config: Optional[FallbackConfig] = None,
) -> AsyncIterator[StreamEvent]:
    """Async: try each fallback model stream in order. Raises the primary error if all fail."""
    candidates = _ordered_candidates(fallback_models, fallback_config=fallback_config, kwargs=kwargs, primary_model_id=primary_model_id)
    for i, fallback in enumerate(candidates):
        emitted = False
        try:
            log_warning(f"Trying fallback model {i + 1}/{len(candidates)}: {fallback.id}")
            iterator = (_initial_stream(fallback, fallback_config, kwargs)
                        if fallback_config is not None and fallback_config.recover_unavailable
                        else _abounded_first_output(fallback, kwargs))
            async with aclosing(iterator):
                async for event in iterator:
                    emitted = emitted or not _is_local_marker(event)
                    if not _is_local_marker(event):
                        from openagent_core.models.account_routes import record_model_result
                        record_model_result(primary_model_id, fallback, event,
                                            streaming=not (fallback_config is not None and fallback_config.recover_unavailable))
                    yield event
            _breaker_record_success(_health_id(fallback))
            _notify_fallback(fallback_config, primary_model_id, fallback.id, primary_error)
            return
        except ModelProviderError as e:
            if emitted:
                raise
            _breaker_record_failure(_health_id(fallback), e)
            log_warning(f"Fallback model '{fallback.id}' also failed: {str(e)}")
            continue
    raise primary_error
