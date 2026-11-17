"""Translation-layer regression tests (dependency-free — no fastapi/curl_cffi).

Run: `PYTHONPATH=. python3 tests/test_translate.py`  (or via pytest).

These lock in the three subscription-path shaping rules, which are the fiddly
bits most likely to regress.
"""

from __future__ import annotations

import base64
import json

from openagent_core.subscriptions.claude import translate as T
from openagent_core.subscriptions.claude.cc_anthropic import BILLING_HEADER_TEXT


def test_openai_to_anthropic_shaping():
    req = {
        "model": "claude-opus-4-8",
        "messages": [
            {"role": "system", "content": "You are Virgil, a helpful agent."},
            {"role": "user", "content": "List my files."},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "call_1", "type": "function",
                 "function": {"name": "mcp_shell_ls", "arguments": '{"path": "."}'}}]},
            {"role": "tool", "tool_call_id": "call_1", "content": "a.txt\nb.txt"},
        ],
        "tools": [{"type": "function", "function": {
            "name": "mcp_shell_ls", "description": "list dir",
            "parameters": {"type": "object", "properties": {"path": {"type": "string"}}}}}],
        "tool_choice": "auto",
    }
    p = T.openai_to_anthropic(req, default_max_tokens=16384, max_tools=19, model_map={})

    # Rule 1: system is billing-only.
    assert p["system"] == [{"type": "text", "text": BILLING_HEADER_TEXT}]
    # Rule 2: identity + persona moved into the leading user message.
    first = p["messages"][0]
    assert first["role"] == "user"
    assert first["content"][0]["text"].startswith("You are Claude Code")
    assert "You are Virgil" in first["content"][0]["text"]
    # Alternating roles after merge.
    assert [m["role"] for m in p["messages"]] == ["user", "assistant", "user"]
    # Rule 3: mcp_ stripped on tool defs and tool_use history.
    assert p["tools"][0]["name"] == "shell_ls"
    tool_uses = [b for m in p["messages"] for b in m["content"]
                 if isinstance(b, dict) and b.get("type") == "tool_use"]
    assert tool_uses[0]["name"] == "shell_ls"


def test_tool_cap():
    req = {"model": "m", "messages": [{"role": "user", "content": "hi"}],
           "tools": [{"type": "function", "function": {"name": f"t{i}", "parameters": {}}}
                     for i in range(30)]}
    p = T.openai_to_anthropic(req, default_max_tokens=100, max_tools=19, model_map={})
    assert len(p["tools"]) == 19


def test_safe_tool_id_unit():
    # Valid ids pass through untouched.
    assert T._safe_tool_id("toolu_01ABC") == "toolu_01ABC"
    assert T._safe_tool_id("call_abc-123") == "call_abc-123"
    # Disallowed chars (: . / space) collapse to '_'.
    assert T._safe_tool_id("a:b.c/d e") == "a_b_c_d_e"
    # Empty/None get a fresh, valid id.
    assert T._safe_tool_id("").startswith("tool_")
    assert T._safe_tool_id(None).startswith("tool_")
    # Deterministic — required so a tool_use id and its tool_result ref stay paired.
    assert T._safe_tool_id("x.y:z") == T._safe_tool_id("x.y:z")


def test_tool_ids_sanitised_and_paired():
    """Anthropic requires tool ids match ^[a-zA-Z0-9_-]+$; OpenAI ids don't.
    Both the assistant tool_use.id and the tool_result.tool_use_id carry the
    same raw id and must be rewritten identically (else Anthropic 400s)."""
    import re
    bad = "fc:get.time/0"
    req = {
        "model": "m",
        "messages": [
            {"role": "user", "content": "time?"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": bad, "type": "function",
                 "function": {"name": "get_time", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": bad, "content": "12:00"},
        ],
    }
    p = T.openai_to_anthropic(req, default_max_tokens=100, max_tools=19, model_map={})
    pat = re.compile(r"^[a-zA-Z0-9_-]+$")
    tool_use = next(b for m in p["messages"] for b in m["content"]
                    if isinstance(b, dict) and b.get("type") == "tool_use")
    tool_result = next(b for m in p["messages"] for b in m["content"]
                       if isinstance(b, dict) and b.get("type") == "tool_result")
    assert pat.match(tool_use["id"]), tool_use["id"]
    assert pat.match(tool_result["tool_use_id"]), tool_result["tool_use_id"]
    assert tool_use["id"] == tool_result["tool_use_id"]  # pairing preserved


def test_tool_pairing_repair_empty_ids():
    """OpenAgent over the streaming path sometimes sends empty tool-call ids.
    The proxy must re-link each tool_result to its positional tool_use so
    Anthropic's 'each tool_result must have a corresponding tool_use' rule holds
    (otherwise: 400 'unexpected tool_use_id')."""
    import re
    req = {
        "model": "m",
        "messages": [
            {"role": "user", "content": "weather and time?"},
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "", "type": "function", "function": {"name": "get_weather", "arguments": "{}"}},
                {"id": "", "type": "function", "function": {"name": "get_time", "arguments": "{}"}},
            ]},
            {"role": "tool", "tool_call_id": "", "content": "sunny"},
            {"role": "tool", "tool_call_id": "", "content": "12:00"},
        ],
    }
    p = T.openai_to_anthropic(req, default_max_tokens=100, max_tools=19, model_map={})
    uses = [b for m in p["messages"] for b in m["content"]
            if isinstance(b, dict) and b.get("type") == "tool_use"]
    results = [b for m in p["messages"] for b in m["content"]
               if isinstance(b, dict) and b.get("type") == "tool_result"]
    assert len(uses) == 2 and len(results) == 2
    use_ids = [b["id"] for b in uses]
    res_ids = [b["tool_use_id"] for b in results]
    # Each tool_result re-linked to its positional tool_use, and all valid ids.
    assert res_ids == use_ids
    pat = re.compile(r"^[a-zA-Z0-9_-]+$")
    assert all(pat.match(x) for x in use_ids + res_ids)


def test_tool_result_image_reaches_claude_as_image_block():
    """A screenshot from OpenAgent must be visible inside Claude's tool result."""
    image = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="
    req = {"model": "claude-opus-4-8", "messages": [
        {"role": "user", "content": "What is on screen?"},
        {"role": "assistant", "content": "", "tool_calls": [{
            "id": "shot_1", "type": "function",
            "function": {"name": "computer_control_computer", "arguments": '{"action":"get_screenshot"}'},
        }]},
        {"role": "tool", "tool_call_id": "shot_1", "content": [
            {"type": "text", "text": "Screenshot attached"},
            {"type": "image_url", "image_url": {"url": image}},
        ]},
    ]}

    payload = T.openai_to_anthropic(req, default_max_tokens=100, max_tools=19, model_map={})
    result = next(block for message in payload["messages"] for block in message["content"]
                  if block.get("type") == "tool_result")
    assert result["tool_use_id"] == "shot_1"
    assert result["content"][0] == {"type": "text", "text": "Screenshot attached"}
    assert result["content"][1]["type"] == "image"
    assert result["content"][1]["source"] == {
        "type": "base64", "media_type": "image/png", "data": "iVBORw0KGgoAAAANSUhEUg==",
    }


def test_anthropic_to_openai():
    ar = {"content": [{"type": "text", "text": "Here you go."},
                      {"type": "tool_use", "id": "tu_1", "name": "shell_ls", "input": {"path": "."}}],
          "stop_reason": "tool_use", "usage": {"input_tokens": 12, "output_tokens": 8}}
    o = T.anthropic_to_openai(ar, "claude-opus-4-8")
    assert o["choices"][0]["finish_reason"] == "tool_calls"
    assert o["choices"][0]["message"]["tool_calls"][0]["function"]["name"] == "shell_ls"
    assert o["usage"]["total_tokens"] == 20


def test_stream_to_openai():
    events = [
        {"type": "message_start", "message": {"id": "m", "role": "assistant"}},
        {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
        {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "Hi"}},
        {"type": "content_block_start", "index": 1,
         "content_block": {"type": "tool_use", "id": "tu_2", "name": "shell_ls"}},
        {"type": "content_block_delta", "index": 1,
         "delta": {"type": "input_json_delta", "partial_json": '{"path"'}},
        {"type": "content_block_delta", "index": 1,
         "delta": {"type": "input_json_delta", "partial_json": ':"."}'}},
        {"type": "message_delta", "delta": {"stop_reason": "tool_use"}},
        {"type": "message_stop"},
    ]
    chunks = list(T.anthropic_stream_to_openai(iter(events), "claude-opus-4-8"))
    joined = "".join(chunks)
    assert chunks[-1] == "data: [DONE]\n\n"
    assert '"role": "assistant"' in joined
    assert '"content": "Hi"' in joined
    assert '"name": "shell_ls"' in joined
    assert '"finish_reason": "tool_calls"' in joined


def test_prompt_caching_breakpoints():
    """Regression guard: every translated request must carry cache_control
    breakpoints, otherwise each tool-calling round re-bills the full context
    at full price (no cache read). See translate.openai_to_anthropic."""
    req = {
        "model": "claude-sonnet-4-6",
        "messages": [
            {"role": "system", "content": "BIG SYSTEM " * 100},
            {"role": "user", "content": "q1"},
            {"role": "assistant", "content": "",
             "tool_calls": [{"id": "t1", "function": {"name": "search", "arguments": "{}"}}]},
            {"role": "tool", "tool_call_id": "t1", "content": "out"},
            {"role": "user", "content": "follow up"},
        ],
        "tools": [
            {"function": {"name": "search", "description": "x", "parameters": {"type": "object"}}},
            {"function": {"name": "fetch", "description": "y", "parameters": {"type": "object"}}},
        ],
    }
    p = T.openai_to_anthropic(req, default_max_tokens=4096, max_tools=19, model_map={})

    def _all_cc(obj):
        out = []
        if isinstance(obj, dict):
            if "cache_control" in obj:
                out.append(obj)
            for v in obj.values():
                out += _all_cc(v)
        elif isinstance(obj, list):
            for v in obj:
                out += _all_cc(v)
        return out

    cc = _all_cc(p)
    # BP1 lead_text, BP2 last tool, BP3 last conversation block
    assert len(cc) == 3, f"expected 3 cache breakpoints, got {len(cc)}"
    assert len(cc) <= 4, "Anthropic allows at most 4 cache_control breakpoints"
    # lead_text (first user message) must be cached
    assert "cache_control" in p["messages"][0]["content"][0]
    # last tool must be cached
    assert "cache_control" in p["tools"][-1]
    # last conversation block must be cached
    assert "cache_control" in p["messages"][-1]["content"][-1]


def test_no_tools_still_caches():
    """A request without tools must still cache lead_text + conversation."""
    req = {"model": "claude-sonnet-4-6",
           "messages": [{"role": "user", "content": "hi"}]}
    p = T.openai_to_anthropic(req, default_max_tokens=4096, max_tools=19, model_map={})
    assert "cache_control" in p["messages"][0]["content"][0]
    assert "tools" not in p


def test_image_url_to_anthropic_block():
    """OpenAI image_url parts (base64 data URL + remote URL) become Anthropic
    image blocks; unparseable ones are dropped, text is preserved."""
    data_url = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUg=="
    req = {
        "model": "claude-opus-4-8",
        "messages": [
            {"role": "user", "content": [
                {"type": "text", "text": "What rooms are in this floor plan?"},
                {"type": "image_url", "image_url": {"url": data_url}},
                {"type": "image_url", "image_url": {"url": "https://example.com/x.jpg"}},
                {"type": "image_url", "image_url": {"url": "ftp://nope"}},  # dropped
            ]},
        ],
    }
    p = T.openai_to_anthropic(req, default_max_tokens=4096, max_tools=19, model_map={})
    blocks = p["messages"][0]["content"]  # leading CC message merged with the user turn
    images = [b for b in blocks if isinstance(b, dict) and b.get("type") == "image"]
    assert len(images) == 2, f"expected 2 image blocks, got {len(images)}"
    b64 = next(b for b in images if b["source"]["type"] == "base64")
    assert b64["source"]["media_type"] == "image/png"
    assert b64["source"]["data"] == "iVBORw0KGgoAAAANSUhEUg=="
    url = next(b for b in images if b["source"]["type"] == "url")
    assert url["source"]["url"] == "https://example.com/x.jpg"
    assert any(isinstance(b, dict) and b.get("type") == "text" and "floor plan" in b["text"]
               for b in blocks)


def test_image_only_user_message():
    """A user turn that is only an image must not collapse to empty text."""
    req = {"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,QQ=="}}]}]}
    p = T.openai_to_anthropic(req, default_max_tokens=100, max_tools=19, model_map={})
    blocks = p["messages"][0]["content"]
    img = next(b for b in blocks if isinstance(b, dict) and b.get("type") == "image")
    assert img["source"]["media_type"] == "image/jpeg"
    assert img["source"]["data"] == "QQ=="


def test_image_media_type_case_insensitive():
    """Media types are case-insensitive (RFC 2045): "image/JPEG" and a ";BASE64"
    marker must be normalized, not mislabeled as png or dropped."""
    upper = T._image_block({"url": "data:image/JPEG;base64,QQ=="})
    assert upper is not None and upper["source"]["media_type"] == "image/jpeg"
    upper_marker = T._image_block({"url": "data:image/png;BASE64,QQ=="})
    assert upper_marker is not None and upper_marker["source"]["media_type"] == "image/png"


def test_file_part_pdf_to_document():
    """OpenAI file parts (inline base64 PDF) become Anthropic document blocks
    alongside any text, with the filename carried as the document title."""
    pdf = "data:application/pdf;base64,JVBERi0xLjQK"  # "%PDF-1.4\n"
    req = {"model": "m", "messages": [
        {"role": "user", "content": [
            {"type": "text", "text": "Read this floor plan."},
            {"type": "file", "file": {"filename": "plan.pdf", "file_data": pdf}},
        ]}]}
    p = T.openai_to_anthropic(req, default_max_tokens=100, max_tools=19, model_map={})
    blocks = p["messages"][0]["content"]
    doc = next(b for b in blocks if isinstance(b, dict) and b.get("type") == "document")
    assert doc["source"] == {"type": "base64", "media_type": "application/pdf", "data": "JVBERi0xLjQK"}
    assert doc["title"] == "plan.pdf"
    assert any(isinstance(b, dict) and b.get("type") == "text" and "floor plan" in b["text"]
               for b in blocks)


def test_file_part_text_to_document():
    """A text/* file becomes a document with a decoded text source."""
    text_b64 = base64.b64encode(b"room: kitchen, floor: 0").decode()
    block = T._file_block({"file_data": f"data:text/plain;base64,{text_b64}"})
    assert block == {
        "type": "document",
        "source": {"type": "text", "media_type": "text/plain", "data": "room: kitchen, floor: 0"},
    }


def test_pdf_via_image_url_routes_to_document():
    """A PDF mis-sent as image_url is still routed to a document block, not an image."""
    block = T._image_block({"url": "data:application/pdf;base64,JVBERi0xLjQK"})
    assert block is not None and block["type"] == "document"
    assert block["source"]["media_type"] == "application/pdf"


def test_file_id_reference_dropped():
    """A file_id reference can't be resolved by the proxy and is dropped, not crashed."""
    assert T._file_block({"file_id": "file_123"}) is None
    assert T._file_block({}) is None


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS  {name}")
    print("\nall translation tests passed")


def test_stream_emits_usage_when_requested():
    """Regression guard: a streaming caller that asks for usage must get it.

    OpenAgent sends stream_options.include_usage on every streaming call, and
    streaming is what the agents actually run. With the final usage chunk
    dropped, every Claude-served turn reported ZERO tokens: the cost ledger
    read empty while two agents burned ~1B input tokens on 2026-07-13, and the
    first hard signal was a provider running out of credit.
    """
    events = [
        {"type": "message_start", "message": {"usage": {
            "input_tokens": 1200,
            "output_tokens": 0,
            "cache_read_input_tokens": 8000,
            "cache_creation_input_tokens": 300,
        }}},
        {"type": "content_block_delta", "index": 0,
         "delta": {"type": "text_delta", "text": "Hi"}},
        {"type": "message_delta", "delta": {"stop_reason": "end_turn"},
         "usage": {"output_tokens": 42}},
        {"type": "message_stop"},
    ]
    chunks = list(
        T.anthropic_stream_to_openai(iter(events), "claude-sonnet-4-6", include_usage=True)
    )
    assert chunks[-1] == "data: [DONE]\n\n"

    usage_chunk = json.loads(chunks[-2][len("data: "):])
    assert usage_chunk["choices"] == []
    usage = usage_chunk["usage"]
    # Cache reads are still tokens the model had to be handed — a cached turn
    # is cheaper, not free.
    assert usage["prompt_tokens"] == 1200 + 8000 + 300
    assert usage["completion_tokens"] == 42
    assert usage["total_tokens"] == 1200 + 8000 + 300 + 42
    assert usage["prompt_tokens_details"]["cached_tokens"] == 8000


def test_stream_omits_usage_when_not_requested():
    """Default off: a client that didn't ask must not get an extra chunk."""
    events = [
        {"type": "message_start", "message": {"usage": {"input_tokens": 5, "output_tokens": 0}}},
        {"type": "content_block_delta", "index": 0,
         "delta": {"type": "text_delta", "text": "Hi"}},
        {"type": "message_stop"},
    ]
    chunks = list(T.anthropic_stream_to_openai(iter(events), "claude-sonnet-4-6"))
    assert chunks[-1] == "data: [DONE]\n\n"
    assert all('"usage"' not in c for c in chunks)
