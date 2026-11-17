"""OpenAI Chat Completions <-> Anthropic Messages translation, plus the
subscription-path shaping the consumer OAuth endpoint requires.

Three rules are non-obvious and lifted from the verified path in
``marf/hermes-agent-claude-sub`` (the ``_rewrite_outbound_body`` /
``_is_oauth_token`` branches). Get them wrong and the endpoint 400s:

  1. ``system`` carries ONLY the billing header block. A custom system prompt is
     rejected on the subscription path...
  2. ...so the Claude Code identity line + any caller system/developer prompt are
     moved into a LEADING USER message (the same text as a user turn is accepted).
  3. Tool names lose the ``mcp_`` prefix and the tool list is capped (>= 20 tools
     => 400).

Scope of this scaffold: the text + tool-calling path (streaming and not) plus
multimodal user input — OpenAI ``image_url`` parts (data-URLs and remote URLs)
become Anthropic image blocks, and OpenAI ``file`` parts (inline base64
``file_data`` data-URLs) become Anthropic document blocks: PDFs and text files
are supported. Audio parts and Anthropic-native extras (extended thinking,
citations) are still flagged with TODOs.
"""

from __future__ import annotations

import base64
import json
import logging
import re
import time
import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .cc_anthropic import CLAUDE_CODE_SYSTEM_PREFIX, billing_block

logger = logging.getLogger("claude_sub_proxy.translate")

_MCP_PREFIX = "mcp_"
_SYSTEM_ROLES = ("system", "developer")

# Anthropic constrains tool ids to ^[a-zA-Z0-9_-]+$, but OpenAI tool_call ids
# carry no such restriction. Any other char (':', '.', '/', whitespace, ...)
# must be rewritten or the subscription endpoint 400s on
# ``messages.N.content.0.tool_result.tool_use_id``.
_TOOL_ID_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")


def _upstream_model(model: str, model_map: Dict[str, str]) -> str:
    """Accept legacy family/version aliases without changing their generation."""
    if model in model_map:
        return model_map[model]
    if re.fullmatch(r"(?:opus|sonnet|haiku)-\d+(?:-\d+)*(?:-latest)?", model):
        return "claude-" + model
    return model


def _safe_tool_id(raw: Any) -> str:
    """Map an arbitrary OpenAI tool-call id onto Anthropic's allowed charset.

    Deterministic: the assistant ``tool_use.id`` and its matching
    ``tool_result.tool_use_id`` share the same raw value, so sanitising both
    with this function keeps the pair matched. Empty/None ids get a fresh id.
    """
    s = raw if isinstance(raw, str) else ("" if raw is None else str(raw))
    cleaned = _TOOL_ID_DISALLOWED.sub("_", s)
    if not cleaned:
        cleaned = "tool_" + uuid.uuid4().hex
    if cleaned != s:
        logger.info("sanitised tool id %r -> %r", s, cleaned)
    return cleaned

_STOP_MAP = {
    "end_turn": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "stop_sequence": "stop",
    "pause_turn": "stop",
    "refusal": "stop",
}


def _strip_mcp(name: str) -> str:
    return name[len(_MCP_PREFIX):] if isinstance(name, str) and name.startswith(_MCP_PREFIX) else name


# ── OpenAI request -> Anthropic payload ──────────────────────────────────────
def openai_to_anthropic(
    req: Dict[str, Any],
    *,
    default_max_tokens: int,
    max_tools: int,
    model_map: Dict[str, str],
) -> Dict[str, Any]:
    messages_in = req.get("messages") or []
    persona_parts: List[str] = []
    convo: List[Dict[str, Any]] = []

    for msg in messages_in:
        role = msg.get("role")
        content = msg.get("content")
        if role in _SYSTEM_ROLES:
            persona_parts.append(_flatten_text(content))
        elif role == "user":
            convo.append({"role": "user", "content": _user_content(content)})
        elif role == "assistant":
            convo.append({"role": "assistant", "content": _assistant_content(msg)})
        elif role == "tool":
            # OpenAI-compatible runtimes can put screenshots in a tool message
            # as image_url parts. Claude accepts image/document blocks nested
            # inside tool_result.content; flattening here silently discarded
            # the pixels and left the agent to guess what the screen showed.
            tool_content = _user_content(content) if isinstance(content, list) else (_flatten_text(content) or "(no output)")
            convo.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": _safe_tool_id(msg.get("tool_call_id", "")),
                    "content": tool_content,
                }],
            })
        # unknown roles are ignored

    # Rule 1 + 2: identity + caller persona become a leading user message.
    persona = "\n\n".join(p for p in persona_parts if p).strip()
    lead_text = CLAUDE_CODE_SYSTEM_PREFIX + (("\n\n" + persona) if persona else "")
    convo = [{"role": "user", "content": [{"type": "text", "text": lead_text, "cache_control": {"type": "ephemeral"}}]}] + convo
    convo = _merge_consecutive(convo)
    convo = _repair_tool_pairing(convo)

    # BREAKPOINT (prompt caching): mark the last content block of the last
    # message so Anthropic caches the whole conversation prefix up to here.
    # Without this every tool-calling round re-bills the full context at full
    # price; with it, stable prefix tokens are cache-read at ~10% cost.
    if convo and isinstance(convo[-1].get("content"), list) and convo[-1]["content"]:
        _last_block = convo[-1]["content"][-1]
        if isinstance(_last_block, dict) and _last_block.get("type") in ("text", "tool_result", "tool_use"):
            _last_block["cache_control"] = {"type": "ephemeral"}

    payload: Dict[str, Any] = {
        "model": _upstream_model(req.get("model", ""), model_map),
        "max_tokens": int(req.get("max_tokens") or req.get("max_completion_tokens") or default_max_tokens),
        "system": [billing_block()],
        "messages": convo,
    }
    if req.get("temperature") is not None:
        payload["temperature"] = req["temperature"]
    if req.get("top_p") is not None:
        payload["top_p"] = req["top_p"]
    if req.get("stop"):
        payload["stop_sequences"] = req["stop"] if isinstance(req["stop"], list) else [req["stop"]]

    tools = req.get("tools")
    if tools:
        a_tools: List[Dict[str, Any]] = []
        for t in tools:
            fn = t.get("function") if isinstance(t, dict) else None
            if not fn:
                continue
            a_tools.append({
                "name": _strip_mcp(fn.get("name", "")),
                "description": fn.get("description", "") or "",
                "input_schema": fn.get("parameters") or {"type": "object", "properties": {}},
            })
        if len(a_tools) > max_tools:
            a_tools = a_tools[:max_tools]  # Rule 3 — silent cap; >= 20 => 400
        if a_tools:
            # Prompt caching: mark the last tool so the entire tool-schema block
            # (identical every round) is cached instead of re-billed each call.
            a_tools[-1] = {**a_tools[-1], "cache_control": {"type": "ephemeral"}}
            payload["tools"] = a_tools
            tc = _tool_choice(req.get("tool_choice"))
            if tc is not None:
                payload["tool_choice"] = tc
    return payload


def _flatten_text(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        out = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                out.append(part.get("text", ""))
            # TODO: image/audio parts dropped on the text path.
        return "".join(out)
    return str(content)


def _user_content(content: Any) -> List[Dict[str, Any]]:
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    blocks: List[Dict[str, Any]] = []
    if isinstance(content, list):
        for part in content:
            if not isinstance(part, dict):
                continue
            ptype = part.get("type")
            if ptype == "text":
                blocks.append({"type": "text", "text": part.get("text", "")})
            elif ptype == "image_url":
                block = _image_block(part.get("image_url"))
                if block is not None:
                    blocks.append(block)
            elif ptype == "file":
                block = _file_block(part.get("file"))
                if block is not None:
                    blocks.append(block)
            # other part types (input_audio) are dropped — unsupported.
    if not blocks:
        blocks = [{"type": "text", "text": _flatten_text(content)}]
    return blocks


_IMAGE_MEDIA_TYPES = ("image/jpeg", "image/png", "image/gif", "image/webp")


def _parse_data_url(url: str) -> Optional[Tuple[str, str]]:
    """('media_type', 'base64-data') for a base64 ``data:`` URL, else None.
    The media type is normalized to lowercase (RFC 2045 case-insensitivity)."""
    if not url.startswith("data:"):
        return None
    header, _, data = url.partition(",")
    if not data:
        return None
    meta = header[len("data:"):]                  # e.g. "image/png;base64"
    if ";base64" not in meta.lower():
        return None                                # url-encoded data URLs unsupported
    media_type = (meta.split(";", 1)[0].strip() or "application/octet-stream").lower()
    return media_type, data


def _block_for_media(
    media_type: str, data: str, *, filename: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Anthropic content block for inline base64 of ``media_type``: images become
    an image block; PDF and text/* become a document block; anything else
    (audio, octet-stream, ...) is unsupported and returns None (dropped)."""
    if media_type in _IMAGE_MEDIA_TYPES:
        return {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": data}}
    if media_type == "application/pdf":
        block: Dict[str, Any] = {
            "type": "document",
            "source": {"type": "base64", "media_type": "application/pdf", "data": data},
        }
        if filename:
            block["title"] = filename
        return block
    if media_type.startswith("text/"):
        try:
            text = base64.b64decode(data).decode("utf-8", "replace")
        except Exception:
            return None
        block = {"type": "document", "source": {"type": "text", "media_type": "text/plain", "data": text}}
        if filename:
            block["title"] = filename
        return block
    return None


def _image_block(image_url: Any) -> Optional[Dict[str, Any]]:
    """OpenAI ``image_url`` part -> Anthropic block. Handles base64 data URLs
    (images, plus PDFs some clients mis-send here) and remote http(s) image URLs;
    returns None for anything unparseable so the rest of the message goes through."""
    if isinstance(image_url, dict):
        url = image_url.get("url")
    elif isinstance(image_url, str):
        url = image_url
    else:
        url = None
    if not isinstance(url, str) or not url:
        return None
    if url.startswith(("http://", "https://")):
        return {"type": "image", "source": {"type": "url", "url": url}}
    parsed = _parse_data_url(url)
    if parsed is None:
        return None
    media_type, data = parsed
    block = _block_for_media(media_type, data)
    if block is not None:
        return block
    # image_url implies an image; best-effort label unknown types as png so a
    # valid image with an odd media type still goes through.
    return {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": data}}


def _file_block(file_part: Any) -> Optional[Dict[str, Any]]:
    """OpenAI ``file`` part -> Anthropic document/image block. Reads the inline
    ``file_data`` base64 data URL (PDF, text, or image). ``file_id`` references
    cannot be resolved by the proxy and are dropped."""
    if not isinstance(file_part, dict):
        return None
    file_data = file_part.get("file_data")
    if not isinstance(file_data, str) or not file_data:
        return None
    parsed = _parse_data_url(file_data)
    if parsed is None:
        return None
    media_type, data = parsed
    filename = file_part.get("filename")
    return _block_for_media(media_type, data, filename=filename if isinstance(filename, str) else None)


def _assistant_content(msg: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = []
    text = _flatten_text(msg.get("content"))
    if text:
        blocks.append({"type": "text", "text": text})
    for tc in (msg.get("tool_calls") or []):
        fn = tc.get("function", {})
        try:
            args = json.loads(fn.get("arguments") or "{}")
        except Exception:
            args = {}
        blocks.append({
            "type": "tool_use",
            "id": _safe_tool_id(tc.get("id", "")),
            "name": _strip_mcp(fn.get("name", "")),
            "input": args,
        })
    if not blocks:
        blocks = [{"type": "text", "text": "(empty)"}]  # Anthropic rejects empty blocks
    return blocks


def _merge_consecutive(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Anthropic requires alternating roles; merge same-role neighbours by
    concatenating their content-block lists. Handles the leading-user message
    colliding with a real first user turn, and runs of tool results."""
    merged: List[Dict[str, Any]] = []
    for m in messages:
        content = m["content"]
        if not isinstance(content, list):
            content = [{"type": "text", "text": str(content)}]
        if merged and merged[-1]["role"] == m["role"]:
            merged[-1]["content"].extend(content)
        else:
            merged.append({"role": m["role"], "content": list(content)})
    return merged


def _repair_tool_pairing(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Anthropic requires every tool_result.tool_use_id to match a tool_use.id
    in the preceding assistant message. OpenAI callers (notably OpenAgent over
    the streaming path) sometimes omit tool-call ids; _safe_tool_id then fills
    each with a fresh per-call id, so the tool_use and its tool_result end up
    with *different* ids and Anthropic 400s ("unexpected tool_use_id ... must
    have a corresponding tool_use block").

    Re-link by position: tool_results follow their assistant message in order,
    so the i-th tool_result of a user turn maps to the i-th tool_use of the most
    recent assistant turn. Untouched when ids already match (the normal case)."""
    prev_ids: List[str] = []
    for m in messages:
        content = m.get("content")
        if not isinstance(content, list):
            continue
        if m.get("role") == "assistant":
            ids = [b.get("id") for b in content
                   if isinstance(b, dict) and b.get("type") == "tool_use"]
            if ids:
                prev_ids = ids
        elif m.get("role") == "user":
            i = 0
            for b in content:
                if isinstance(b, dict) and b.get("type") == "tool_result":
                    if i < len(prev_ids) and b.get("tool_use_id") not in prev_ids:
                        b["tool_use_id"] = prev_ids[i]
                    i += 1
    return messages


def _tool_choice(tc: Any) -> Optional[Dict[str, Any]]:
    if tc in (None, "auto"):
        return {"type": "auto"}
    if tc == "required":
        return {"type": "any"}
    if tc == "none":
        return {"type": "none"}
    if isinstance(tc, dict):
        name = (tc.get("function") or {}).get("name")
        if name:
            return {"type": "tool", "name": _strip_mcp(name)}
    return {"type": "auto"}


# ── Anthropic response -> OpenAI (non-streaming) ─────────────────────────────
def anthropic_to_openai(resp: Dict[str, Any], model: str) -> Dict[str, Any]:
    text_parts: List[str] = []
    tool_calls: List[Dict[str, Any]] = []
    for blk in resp.get("content", []):
        if blk.get("type") == "text":
            text_parts.append(blk.get("text", ""))
        elif blk.get("type") == "tool_use":
            tool_calls.append({
                "id": blk.get("id", ""),
                "type": "function",
                "function": {
                    "name": blk.get("name", ""),
                    "arguments": json.dumps(blk.get("input", {})),
                },
            })
    message: Dict[str, Any] = {"role": "assistant", "content": "".join(text_parts) or None}
    if tool_calls:
        message["tool_calls"] = tool_calls
    usage = resp.get("usage", {}) or {}
    in_tok = usage.get("input_tokens", 0) or 0
    out_tok = usage.get("output_tokens", 0) or 0
    return {
        "id": "chatcmpl-" + uuid.uuid4().hex,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": _STOP_MAP.get(resp.get("stop_reason"), "stop"),
        }],
        "usage": {
            "prompt_tokens": in_tok,
            "completion_tokens": out_tok,
            "total_tokens": in_tok + out_tok,
        },
    }


# ── Anthropic stream -> OpenAI SSE chunks ────────────────────────────────────
def anthropic_stream_to_openai(
    events: Iterator[Dict[str, Any]],
    model: str,
    include_usage: bool = False,
) -> Iterator[str]:
    """Translate an Anthropic event stream into OpenAI SSE chunks.

    ``include_usage`` mirrors OpenAI's ``stream_options.include_usage``: emit a
    final chunk with an empty ``choices`` list and the turn's token usage, just
    before ``[DONE]``.

    This is not cosmetic. A caller that asks for usage and never gets it does
    not see an error — it sees a turn that cost zero. OpenAgent asks for it on
    every streaming call, and every streaming call is what the agents actually
    run, so with this dropped its cost ledger read zero for real, expensive
    traffic. On 2026-07-13 two agents burned ~1B input tokens against an empty
    ledger, and the first hard signal was a provider running out of credit.

    Anthropic reports input tokens on ``message_start`` and the running output
    count on each ``message_delta``, so both are already on the wire — they were
    simply being thrown away here.
    """
    cid = "chatcmpl-" + uuid.uuid4().hex
    created = int(time.time())

    def _chunk(delta: Dict[str, Any], finish: Optional[str] = None) -> str:
        return "data: " + json.dumps({
            "id": cid,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
        }) + "\n\n"

    tool_idx_by_block: Dict[int, int] = {}  # anthropic block index -> openai tool index
    next_tool_idx = 0
    finish_reason = "stop"
    started = False
    in_tok = 0
    out_tok = 0
    cache_read = 0
    cache_write = 0

    for ev in events:
        et = ev.get("type")
        if et == "message_start":
            usage = ((ev.get("message") or {}).get("usage")) or {}
            in_tok = int(usage.get("input_tokens") or 0)
            out_tok = int(usage.get("output_tokens") or 0)
            cache_read = int(usage.get("cache_read_input_tokens") or 0)
            cache_write = int(usage.get("cache_creation_input_tokens") or 0)
            yield _chunk({"role": "assistant"})
            started = True
        elif et == "content_block_start":
            blk = ev.get("content_block", {})
            if blk.get("type") == "tool_use":
                idx = ev.get("index", 0)
                tool_idx_by_block[idx] = next_tool_idx
                yield _chunk({"tool_calls": [{
                    "index": next_tool_idx,
                    "id": blk.get("id", ""),
                    "type": "function",
                    "function": {"name": blk.get("name", ""), "arguments": ""},
                }]})
                next_tool_idx += 1
        elif et == "content_block_delta":
            d = ev.get("delta", {})
            if d.get("type") == "text_delta":
                if not started:
                    yield _chunk({"role": "assistant"})
                    started = True
                yield _chunk({"content": d.get("text", "")})
            elif d.get("type") == "input_json_delta":
                idx = ev.get("index", 0)
                yield _chunk({"tool_calls": [{
                    "index": tool_idx_by_block.get(idx, 0),
                    "function": {"arguments": d.get("partial_json", "")},
                }]})
        elif et == "message_delta":
            sr = (ev.get("delta") or {}).get("stop_reason")
            if sr:
                finish_reason = _STOP_MAP.get(sr, "stop")
            # Anthropic reports the running output count here, and (on some
            # turns) a corrected input count. Last one wins.
            usage = ev.get("usage") or {}
            if usage.get("output_tokens") is not None:
                out_tok = int(usage["output_tokens"] or 0)
            if usage.get("input_tokens") is not None:
                in_tok = int(usage["input_tokens"] or 0)
        elif et == "message_stop":
            break

    yield _chunk({}, finish=finish_reason)

    if include_usage:
        # OpenAI's shape: a final chunk with no choices, carrying the totals.
        # Cache reads are input tokens the model still had to be given — count
        # them in prompt_tokens (and break them out, as OpenAI does) so a
        # cached turn doesn't look free.
        usage_payload: Dict[str, Any] = {
            "prompt_tokens": in_tok + cache_read + cache_write,
            "completion_tokens": out_tok,
            "total_tokens": in_tok + cache_read + cache_write + out_tok,
        }
        if cache_read or cache_write:
            usage_payload["prompt_tokens_details"] = {"cached_tokens": cache_read}
        yield "data: " + json.dumps({
            "id": cid,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [],
            "usage": usage_payload,
        }) + "\n\n"

    yield "data: [DONE]\n\n"
