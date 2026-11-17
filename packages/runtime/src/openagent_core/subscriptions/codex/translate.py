"""OpenAI Chat Completions <-> ChatGPT Codex Responses translation.

The Codex backend speaks the Responses API, always streams, and is stricter
than the public one. What that costs the translation:

  1. ``system``/``developer`` messages collapse into ``instructions``. Unlike
     the Anthropic subscription path, arbitrary instructions ARE accepted — no
     identity line has to be smuggled into a user turn.
  2. ``temperature``, ``top_p``, ``max_tokens`` and friends are dropped: the
     endpoint 400s on them. Reasoning ``effort`` is the only generation knob,
     so it is also exposed through synthetic ``model:effort`` ids for callers
     that can only send a model string.
  3. ``store=false`` means the backend remembers nothing between turns. The
     model's reasoning survives only if we hand its encrypted blobs back, which
     is what ``ReasoningCache`` is for.

Scope: the text + tool-calling path (streamed and aggregated) plus image input.
Audio parts and Responses-native extras (web_search, computer_use) are not
translated.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid
from collections import OrderedDict
from typing import Any, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger("codex_sub_proxy.translate")

_SYSTEM_ROLES = ("system", "developer")
_VALID_EFFORTS = ("none", "minimal", "low", "medium", "high", "xhigh")
_TOOL_NAME_DISALLOWED = re.compile(r"[^a-zA-Z0-9_-]")
# Server-side tools the subscription exposes. They carry no function schema —
# the caller just declares the type and the backend runs them, so they pass
# through untouched instead of going through the function-tool flattening.
_BUILTIN_TOOL_TYPES = ("image_generation", "web_search", "web_search_preview")

DEFAULT_INSTRUCTIONS = "You are a helpful assistant."


def _now() -> int:
    return int(time.time())


def _completion_id() -> str:
    return "chatcmpl-" + uuid.uuid4().hex[:24]


# ── model + effort ───────────────────────────────────────────────────────────
def split_model(
    model: str,
    *,
    model_map: Optional[Dict[str, str]] = None,
    default_effort: str = "medium",
) -> Tuple[str, str]:
    """Resolve a caller-supplied model id into ``(upstream_model, effort)``.

    Accepts ``gpt-5.6-sol``, ``gpt-5.6-sol:high`` and (for callers that can't
    send a colon) ``gpt-5.6-sol-high``. An explicit ``model_map`` entry wins
    over all of it.
    """
    raw = (model or "").strip()
    mapped = (model_map or {}).get(raw)
    if mapped:
        raw = mapped
    effort = ""
    if ":" in raw:
        raw, _, suffix = raw.partition(":")
        effort = suffix.strip().lower()
    else:
        for candidate in _VALID_EFFORTS:
            if raw.lower().endswith(f"-{candidate}"):
                raw = raw[: -(len(candidate) + 1)]
                effort = candidate
                break
    if effort not in _VALID_EFFORTS:
        effort = ""
    return raw.strip(), effort or default_effort


def _effort_from_request(req: Dict[str, Any]) -> str:
    """OpenAI's own reasoning knobs, in the order clients actually send them."""
    for key in ("reasoning_effort", "reasoning"):
        value = req.get(key)
        if isinstance(value, dict):
            value = value.get("effort")
        if isinstance(value, str) and value.strip().lower() in _VALID_EFFORTS:
            return value.strip().lower()
    return ""


# ── request: OpenAI -> Responses ─────────────────────────────────────────────
def _flatten_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict):
                if part.get("type") in ("text", "input_text", "output_text"):
                    parts.append(str(part.get("text") or ""))
        return "\n".join(p for p in parts if p)
    if content is None:
        return ""
    return str(content)


def _user_content(content: Any) -> List[Dict[str, Any]]:
    if isinstance(content, str):
        return [{"type": "input_text", "text": content}]
    blocks: List[Dict[str, Any]] = []
    for part in content or []:
        if isinstance(part, str):
            blocks.append({"type": "input_text", "text": part})
            continue
        if not isinstance(part, dict):
            continue
        kind = part.get("type")
        if kind in ("text", "input_text"):
            blocks.append({"type": "input_text", "text": str(part.get("text") or "")})
        elif kind in ("image_url", "input_image"):
            url = part.get("image_url")
            if isinstance(url, dict):
                url = url.get("url")
            if url:
                blocks.append({"type": "input_image", "image_url": str(url)})
        else:
            # Audio and file parts have no Codex equivalent; keep the turn
            # intact rather than dropping it silently on the floor.
            logger.info("unsupported content part %r reduced to text", kind)
            text = _flatten_text([part])
            if text:
                blocks.append({"type": "input_text", "text": text})
    return blocks or [{"type": "input_text", "text": ""}]


def sanitize_tool_name(name: Any) -> str:
    """Map an arbitrary OpenAI tool name onto the charset Responses allows."""
    raw = name if isinstance(name, str) else ("" if name is None else str(name))
    cleaned = _TOOL_NAME_DISALLOWED.sub("_", raw)
    return cleaned or "tool"


def _tools_to_responses(req: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Flatten OpenAI tool defs and return them plus a sanitized->original map."""
    tools: List[Dict[str, Any]] = []
    name_map: Dict[str, str] = {}
    for tool in req.get("tools") or []:
        if not isinstance(tool, dict):
            continue
        if tool.get("type") in _BUILTIN_TOOL_TYPES:
            tools.append({k: v for k, v in tool.items() if v is not None})
            continue
        fn = tool.get("function") if tool.get("type") == "function" else tool
        if not isinstance(fn, dict) or not fn.get("name"):
            continue
        original = str(fn["name"])
        safe = sanitize_tool_name(original)
        if safe != original:
            name_map[safe] = original
        tools.append(
            {
                "type": "function",
                "name": safe,
                "description": str(fn.get("description") or ""),
                "strict": bool(fn.get("strict") or False),
                "parameters": fn.get("parameters") or {"type": "object", "properties": {}},
            }
        )
    return tools, name_map


def _tool_choice(req: Dict[str, Any], name_map: Dict[str, str]) -> Any:
    choice = req.get("tool_choice")
    if choice in (None, "auto", "none", "required"):
        return choice or "auto"
    if isinstance(choice, dict):
        if choice.get("type") in _BUILTIN_TOOL_TYPES:
            return {"type": choice["type"]}
        fn = choice.get("function") or choice
        name = fn.get("name")
        if name:
            return {"type": "function", "name": sanitize_tool_name(name)}
    return "auto"


def _assistant_items(msg: Dict[str, Any], name_map: Dict[str, str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    text = _flatten_text(msg.get("content"))
    if text:
        items.append(
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            }
        )
    for call in msg.get("tool_calls") or []:
        if not isinstance(call, dict):
            continue
        fn = call.get("function") or {}
        items.append(
            {
                "type": "function_call",
                "name": sanitize_tool_name(fn.get("name")),
                "arguments": fn.get("arguments") or "{}",
                "call_id": str(call.get("id") or f"call_{uuid.uuid4().hex[:24]}"),
            }
        )
    return items


def prompt_cache_key(instructions: str, items: List[Dict[str, Any]]) -> str:
    """A stable key for the conversation prefix.

    Chat Completions is stateless, so there is no conversation id to reuse.
    Hashing the instructions plus the first user turn gives every follow-up call
    in the same conversation the same key, which is what earns the cached-input
    discount; a different conversation hashes differently and gets its own.
    """
    head = json.dumps(items[:1], sort_keys=True) if items else ""
    digest = hashlib.sha256((instructions + "\x00" + head).encode("utf-8")).hexdigest()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, digest))


class ReasoningCache:
    """Remembers the encrypted reasoning items a turn produced, keyed by the
    tool call it led to.

    With ``store=false`` the backend keeps no state, so a multi-step tool loop
    would otherwise make the model re-derive its plan from scratch on every
    round. When the caller sends back an assistant turn containing tool calls,
    we splice the matching reasoning items in front of them.
    """

    def __init__(self, max_entries: int = 512):
        self._entries: "OrderedDict[str, List[Dict[str, Any]]]" = OrderedDict()
        self._max = max(1, max_entries)

    def put(self, call_ids: List[str], reasoning_items: List[Dict[str, Any]]) -> None:
        if not call_ids or not reasoning_items:
            return
        for call_id in call_ids:
            if not call_id:
                continue
            self._entries[call_id] = reasoning_items
            self._entries.move_to_end(call_id)
        while len(self._entries) > self._max:
            self._entries.popitem(last=False)

    def get(self, call_id: str) -> List[Dict[str, Any]]:
        items = self._entries.get(call_id)
        if items:
            self._entries.move_to_end(call_id)
        return items or []


def openai_to_responses(
    req: Dict[str, Any],
    *,
    default_effort: str = "medium",
    reasoning_summary: str = "auto",
    model_map: Optional[Dict[str, str]] = None,
    reasoning_cache: Optional[ReasoningCache] = None,
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """Build the Codex Responses payload. Returns ``(payload, tool_name_map)``."""
    upstream_model, effort = split_model(
        req.get("model", ""), model_map=model_map, default_effort=default_effort
    )
    effort = _effort_from_request(req) or effort

    instructions_parts: List[str] = []
    items: List[Dict[str, Any]] = []
    tools, name_map = _tools_to_responses(req)

    for msg in req.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        role = msg.get("role")
        if role in _SYSTEM_ROLES:
            text = _flatten_text(msg.get("content"))
            if text:
                instructions_parts.append(text)
        elif role == "user":
            items.append({"type": "message", "role": "user", "content": _user_content(msg.get("content"))})
        elif role == "assistant":
            assistant_items = _assistant_items(msg, name_map)
            if reasoning_cache is not None:
                first_call = next(
                    (i["call_id"] for i in assistant_items if i.get("type") == "function_call"), ""
                )
                for reasoning_item in reasoning_cache.get(first_call):
                    items.append(reasoning_item)
            items.extend(assistant_items)
        elif role == "tool":
            items.append(
                {
                    "type": "function_call_output",
                    "call_id": str(msg.get("tool_call_id") or ""),
                    "output": _flatten_text(msg.get("content")) or "(no output)",
                }
            )
        # unknown roles are ignored

    instructions = "\n\n".join(p for p in instructions_parts if p).strip() or DEFAULT_INSTRUCTIONS
    if not items:
        items = [{"type": "message", "role": "user", "content": [{"type": "input_text", "text": ""}]}]

    payload: Dict[str, Any] = {
        "model": upstream_model,
        "instructions": instructions,
        "input": items,
        "tools": tools,
        "tool_choice": _tool_choice(req, name_map),
        "parallel_tool_calls": bool(req.get("parallel_tool_calls", False)),
        "store": False,
        "stream": True,
        "include": ["reasoning.encrypted_content"],
        "prompt_cache_key": prompt_cache_key(instructions, items),
    }
    if effort and effort != "none":
        payload["reasoning"] = {"effort": effort, "summary": reasoning_summary}
    elif effort == "none":
        payload["reasoning"] = {"effort": "none"}
    # NB: temperature / top_p / max_tokens are intentionally NOT forwarded —
    # the Codex endpoint 400s on them ("Unsupported parameter").
    return payload, name_map


# ── response: Responses stream -> OpenAI ─────────────────────────────────────
def _map_usage(usage: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(usage, dict):
        return None
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    in_details = usage.get("input_tokens_details") or {}
    out_details = usage.get("output_tokens_details") or {}
    return {
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": int(usage.get("total_tokens") or (input_tokens + output_tokens)),
        "prompt_tokens_details": {"cached_tokens": int(in_details.get("cached_tokens") or 0)},
        "completion_tokens_details": {"reasoning_tokens": int(out_details.get("reasoning_tokens") or 0)},
    }



def _prepend(
    first: Optional[Tuple[Dict[str, Any], Dict[str, str]]],
    rest: Iterator[Tuple[Dict[str, Any], Dict[str, str]]],
) -> Iterator[Tuple[Dict[str, Any], Dict[str, str]]]:
    if first is not None:
        yield first
    for item in rest:
        yield item

class TurnAssembler:
    """Folds a Responses event stream into OpenAI-shaped output.

    One object serves both modes: ``chunks()`` yields Chat Completions SSE for a
    streaming caller, while ``completion()`` returns the aggregated non-streamed
    body. Both read the same accumulated state, so the two paths cannot drift.
    """

    def __init__(
        self,
        model: str,
        *,
        tool_name_map: Optional[Dict[str, str]] = None,
        emit_reasoning_content: bool = True,
    ):
        self.model = model
        self.tool_name_map = tool_name_map or {}
        self.emit_reasoning_content = emit_reasoning_content
        self.id = _completion_id()
        self.created = _now()
        self.text_parts: List[str] = []
        self.reasoning_parts: List[str] = []
        self.tool_calls: List[Dict[str, Any]] = []
        self.reasoning_items: List[Dict[str, Any]] = []
        self.images: List[Dict[str, Any]] = []
        self.web_searches = 0
        self.usage: Optional[Dict[str, Any]] = None
        self.finish_reason: str = "stop"
        self._tool_index_by_item: Dict[str, int] = {}

    # -- internals ---------------------------------------------------------
    def _original_name(self, name: str) -> str:
        return self.tool_name_map.get(name, name)

    def _chunk(self, delta: Dict[str, Any], finish_reason: Optional[str] = None) -> str:
        payload = {
            "id": self.id,
            "object": "chat.completion.chunk",
            "created": self.created,
            "model": self.model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }
        return "data: " + json.dumps(payload) + "\n\n"

    def _register_tool_call(self, item: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        item_id = str(item.get("id") or item.get("call_id") or uuid.uuid4().hex)
        if item_id in self._tool_index_by_item:
            index = self._tool_index_by_item[item_id]
            call = self.tool_calls[index]
        else:
            index = len(self.tool_calls)
            self._tool_index_by_item[item_id] = index
            call = {
                "id": str(item.get("call_id") or f"call_{uuid.uuid4().hex[:24]}"),
                "type": "function",
                "function": {"name": self._original_name(str(item.get("name") or "")), "arguments": ""},
            }
            self.tool_calls.append(call)
        return index, call

    def _observe(self, event: Dict[str, Any]) -> None:
        """Update accumulated state from one event (mode-independent)."""
        kind = event.get("type")
        if kind == "response.output_item.added":
            item = event.get("item") or {}
            if item.get("type") == "function_call":
                self._register_tool_call(item)
        elif kind == "response.output_text.delta":
            self.text_parts.append(str(event.get("delta") or ""))
        elif kind in ("response.reasoning_summary_text.delta", "response.reasoning_text.delta"):
            self.reasoning_parts.append(str(event.get("delta") or ""))
        elif kind == "response.function_call_arguments.delta":
            item_id = str(event.get("item_id") or "")
            index = self._tool_index_by_item.get(item_id)
            if index is not None:
                self.tool_calls[index]["function"]["arguments"] += str(event.get("delta") or "")
        elif kind == "response.output_item.done":
            item = event.get("item") or {}
            item_type = item.get("type")
            if item_type == "function_call":
                _, call = self._register_tool_call(item)
                # The `.done` item is authoritative: a stream that never sent
                # argument deltas still lands here with the full arguments.
                if item.get("arguments"):
                    call["function"]["arguments"] = str(item["arguments"])
                if item.get("name"):
                    call["function"]["name"] = self._original_name(str(item["name"]))
            elif item_type == "reasoning":
                self.reasoning_items.append(item)
            elif item_type == "image_generation_call":
                # `result` is the whole PNG, base64, typically ~1MB. Kept out
                # of `content` on purpose: a data URL that size wrecks any
                # transcript it lands in.
                self.images.append(
                    {
                        "b64_json": str(item.get("result") or ""),
                        "output_format": item.get("output_format") or "png",
                        "size": item.get("size"),
                        "revised_prompt": item.get("revised_prompt"),
                    }
                )
            elif item_type == "web_search_call":
                self.web_searches += 1
        elif kind in ("response.completed", "response.incomplete"):
            response = event.get("response") or {}
            self.usage = _map_usage(response.get("usage"))
            reason = (response.get("incomplete_details") or {}).get("reason")
            if reason == "max_output_tokens":
                self.finish_reason = "length"
            elif self.tool_calls:
                self.finish_reason = "tool_calls"

    # -- outputs -----------------------------------------------------------
    def chunks(
        self,
        events: Iterator[Tuple[Dict[str, Any], Dict[str, str]]],
        *,
        include_usage: bool = False,
        on_headers: Optional[Any] = None,
    ) -> Iterator[str]:
        emitted_tools: Dict[int, bool] = {}
        headers_seen = False

        # Pull the first event BEFORE emitting anything. The upstream status
        # only materialises on that first read, and a caller that has already
        # received a chunk can no longer be failed over to another account.
        stream = iter(events)
        try:
            first = next(stream)
        except StopIteration:
            first = None

        yield self._chunk({"role": "assistant", "content": ""})

        for event, headers in _prepend(first, stream):
            if on_headers is not None and not headers_seen:
                headers_seen = True
                on_headers(headers)
            kind = event.get("type")
            self._observe(event)

            if kind == "response.output_text.delta":
                yield self._chunk({"content": str(event.get("delta") or "")})
            elif (
                self.emit_reasoning_content
                and kind in ("response.reasoning_summary_text.delta", "response.reasoning_text.delta")
            ):
                yield self._chunk({"reasoning_content": str(event.get("delta") or "")})
            elif kind == "response.output_item.added":
                item = event.get("item") or {}
                if item.get("type") == "function_call":
                    index, call = self._register_tool_call(item)
                    if call["function"]["name"] and not emitted_tools.get(index):
                        emitted_tools[index] = True
                        yield self._chunk(
                            {
                                "tool_calls": [
                                    {
                                        "index": index,
                                        "id": call["id"],
                                        "type": "function",
                                        "function": {"name": call["function"]["name"], "arguments": ""},
                                    }
                                ]
                            }
                        )
            elif kind == "response.function_call_arguments.delta":
                item_id = str(event.get("item_id") or "")
                index = self._tool_index_by_item.get(item_id)
                if index is None:
                    continue
                if not emitted_tools.get(index):
                    emitted_tools[index] = True
                    call = self.tool_calls[index]
                    yield self._chunk(
                        {
                            "tool_calls": [
                                {
                                    "index": index,
                                    "id": call["id"],
                                    "type": "function",
                                    "function": {"name": call["function"]["name"], "arguments": ""},
                                }
                            ]
                        }
                    )
                yield self._chunk(
                    {"tool_calls": [{"index": index, "function": {"arguments": str(event.get("delta") or "")}}]}
                )
            elif kind == "response.output_item.done":
                item = event.get("item") or {}
                if item.get("type") == "image_generation_call" and self.images:
                    yield self._chunk({"images": [self.images[-1]]})
                    continue
                if item.get("type") != "function_call":
                    continue
                index, call = self._register_tool_call(item)
                if not emitted_tools.get(index):
                    # No argument deltas ever arrived — emit the call whole.
                    emitted_tools[index] = True
                    yield self._chunk(
                        {
                            "tool_calls": [
                                {
                                    "index": index,
                                    "id": call["id"],
                                    "type": "function",
                                    "function": {
                                        "name": call["function"]["name"],
                                        "arguments": call["function"]["arguments"],
                                    },
                                }
                            ]
                        }
                    )

        yield self._chunk({}, finish_reason=self.finish_reason)
        if include_usage and self.usage:
            yield "data: " + json.dumps(
                {
                    "id": self.id,
                    "object": "chat.completion.chunk",
                    "created": self.created,
                    "model": self.model,
                    "choices": [],
                    "usage": self.usage,
                }
            ) + "\n\n"
        yield "data: [DONE]\n\n"

    def consume(
        self,
        events: Iterator[Tuple[Dict[str, Any], Dict[str, str]]],
        *,
        on_headers: Optional[Any] = None,
    ) -> None:
        """Fold the whole stream into state, emitting nothing."""
        headers_seen = False
        for event, headers in events:
            if on_headers is not None and not headers_seen:
                headers_seen = True
                on_headers(headers)
            self._observe(event)

    def completion(self) -> Dict[str, Any]:
        message: Dict[str, Any] = {"role": "assistant", "content": "".join(self.text_parts) or None}
        if self.reasoning_parts and self.emit_reasoning_content:
            message["reasoning_content"] = "".join(self.reasoning_parts)
        if self.tool_calls:
            message["tool_calls"] = self.tool_calls
        if self.images:
            message["images"] = self.images
        body: Dict[str, Any] = {
            "id": self.id,
            "object": "chat.completion",
            "created": self.created,
            "model": self.model,
            "choices": [{"index": 0, "message": message, "finish_reason": self.finish_reason}],
        }
        if self.usage:
            body["usage"] = self.usage
        return body

    def cache_reasoning(self, cache: Optional[ReasoningCache]) -> None:
        if cache is None or not self.reasoning_items or not self.tool_calls:
            return
        cache.put([call["id"] for call in self.tool_calls], self.reasoning_items)


def image_request_to_responses(
    req: Dict[str, Any],
    *,
    default_model: str,
    model_map: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build a Responses payload that must produce an image.

    OpenAI's images endpoint has no equivalent here: the subscription generates
    images through the ``image_generation`` server-side tool on the normal
    responses path, so the request is a chat turn with that tool forced.

    ``size`` is honoured the way the ChatGPT app honours it: by ASKING, in the
    prompt. The tool's own ``size`` field is ignored by this backend (1024x1536
    on the field still came back 1254x1254), but the same dimensions written
    into the prompt come back exactly. Square is the one exception — it has a
    native 1254x1254 and stays there. ``quality`` is not controllable at all.
    """
    prompt = str(req.get("prompt") or "").strip()
    size = str(req.get("size") or "").strip().lower()
    if size and size not in ("auto", "1024x1024"):
        prompt = f"{prompt}\n\nRender the image at exactly {size} pixels."
    upstream_model, _ = split_model(
        req.get("model") or default_model, model_map=model_map, default_effort="low"
    )
    tool: Dict[str, Any] = {"type": "image_generation"}
    if req.get("output_format"):
        tool["output_format"] = req["output_format"]
    # Sent as well even though this backend ignores it: harmless here, and
    # correct if the endpoint ever starts reading it.
    if size and size != "auto":
        tool["size"] = size
    return {
        "model": upstream_model,
        "instructions": (
            "Generate the image the user asks for with the image_generation tool. "
            "Do not ask clarifying questions and do not describe the image in words."
        ),
        "input": [
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": prompt}]}
        ],
        "tools": [tool],
        "tool_choice": {"type": "image_generation"},
        "parallel_tool_calls": False,
        "store": False,
        "stream": True,
        "include": [],
        "reasoning": {"effort": "low"},
        "prompt_cache_key": prompt_cache_key(prompt, []),
    }
