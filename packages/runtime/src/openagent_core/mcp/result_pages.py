"""Model-facing pages over an already published structured tool result."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from typing import Any

from openagent_core.contracts import ResourceRef, SessionRef
from openagent_core.mcp.pagination import collection_page, page_character_budget
from openagent_core.runtime import current_run_id, current_runtime

_PAGE_ITEMS = 20
_PROVIDER_PAGING = {
    "next_cursor", "nextCursor", "next_page_token", "nextPageToken",
    "has_more", "hasMore", "pagination",
}
_MEDIA_FIELDS = {"images", "videos", "audios", "files"}
_MCP_MEDIA_FIELDS = {"image": "data", "audio": "data", "resource": "resource", "resource_link": "uri"}


def _structured_payload(value: Any) -> Any:
    if isinstance(value, dict):
        if value.get("isError") or value.get("child_session_id") or value.get("child_run_id"):
            return None
        if isinstance(value.get("mcp_result"), dict):
            if any(value.get(key) for key in _MEDIA_FIELDS):
                return None
            return _structured_payload(value["mcp_result"])
        blocks = value.get("content")
        if "structuredContent" in value or (
            isinstance(blocks, list) and blocks
            and all(isinstance(block, dict) and "type" in block for block in blocks)
        ):
            # Leave binary and resource blocks to the existing media presenter.
            if blocks and (
                not isinstance(blocks, list)
                or any(not isinstance(block, dict) or block.get("type") != "text" for block in blocks)
                or any(value.get(key) for key in _MEDIA_FIELDS)
            ):
                return None
            text = _structured_payload(blocks[0].get("text")) if blocks and len(blocks) == 1 else None
            structured = value.get("structuredContent")
            if structured is None:
                return text if isinstance(text, (dict, list)) else value
            if not blocks or text == structured or (
                isinstance(structured, dict) and set(structured) == {"result"}
                and text == structured["result"]
            ):
                return structured
            # Independent text may contain status or instructions. Keep it in
            # the projection instead of treating it as a redundant JSON echo.
            return value
    if isinstance(value, str) and value.lstrip().startswith(("[", "{")):
        try:
            return json.loads(value)
        except (ValueError, RecursionError):
            return None
    return value


def _lists(value: Any, path: tuple[str, ...] = ()):
    if isinstance(value, list):
        if not any(
            isinstance(item, dict) and isinstance(item.get("type"), str)
            and _MCP_MEDIA_FIELDS.get(item["type"]) in item
            for item in value
        ):
            yield path, value
    elif isinstance(value, dict) and not value.keys() & _PROVIDER_PAGING:
        for key, child in value.items():
            if key != "_meta" and isinstance(child, (list, dict)):
                yield from _lists(child, (*path, key))


class ModelResultPages:
    """Continue a page without rerunning the tool or copying its persisted output."""

    def __init__(self):
        self._key = secrets.token_bytes(32)

    def _signature(self, payload, context, run_id):
        scope = json.dumps([run_id, context.coalescing_key]).encode()
        return hmac.digest(self._key, scope + b"\0" + payload, hashlib.sha256)

    def _cursor(self, context, run_id, tool_ref, invocation, path, offset):
        path_hash = self._path_hash(path)
        payload = json.dumps([tool_ref, invocation, path_hash, offset], separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(payload + self._signature(payload, context, run_id)).decode()

    @staticmethod
    def _path_hash(path):
        return hashlib.sha256(json.dumps(path, separators=(",", ":")).encode()).hexdigest()

    def first(self, value, *, context, tool_ref, invocation):
        runtime, run_id = current_runtime(), current_run_id()
        if run_id is None or not callable(getattr(runtime.services.store, "read_tool_invocation", None)):
            return value
        structured = _structured_payload(value)
        collections = [(path, items) for path, items in _lists(structured) if items]
        if not collections:
            return value
        budget = max(100, page_character_budget() // len(collections))
        paging = {}
        for path, items in collections:
            page = collection_page(items, 0, _PAGE_ITEMS, budget)
            if len(page) < len(items):
                paging[path] = (page, {
                    "path": list(path),
                    "total": len(items),
                    "result_cursor": self._cursor(context, run_id, tool_ref, invocation, path, len(page)),
                })
        if not paging:
            return value

        def project(part, path=()):
            if path in paging:
                return paging[path][0]
            if isinstance(part, dict):
                return {key: project(child, (*path, key)) for key, child in part.items()}
            return part

        return {
            "result": project(structured),
            "openagent_pages": [page_info for _, page_info in paging.values()],
        }

    async def next(self, cursor, *, catalog, context):
        runtime, run_id = current_runtime(), current_run_id()
        read = getattr(runtime.services.store, "read_tool_invocation", None)
        if run_id is None or not callable(read):
            raise PermissionError("Tool result pages require the original active run")
        try:
            if not isinstance(cursor, str) or not 1 <= len(cursor) <= 4096:
                raise ValueError()
            raw = base64.b64decode(cursor, altchars=b"-_", validate=True)
            payload, signature = raw[:-32], raw[-32:]
            if not hmac.compare_digest(signature, self._signature(payload, context, run_id)):
                raise ValueError()
            tool_ref, invocation, path_hash, offset = json.loads(payload)
        except (ValueError, TypeError) as exc:
            raise ValueError("Invalid or expired tool result cursor") from exc

        session = ResourceRef("session", context.tenant_id, context.session_id)

        async def authorize():
            descriptor = await catalog.describe_tool(tool_ref, context)
            tool = ResourceRef("tool", context.tenant_id, f"{descriptor.source_id}/{descriptor.name}")
            await runtime.authorize(context, "session.read", session, audience=context.audience)
            await runtime.authorize(context, "tool.publish", tool, audience=context.audience)

        await authorize()
        reference = SessionRef(context.authority.authority, context.tenant_id, context.agent_id, context.session_id)
        saved = await read(reference, invocation, context)
        if saved.get("run_id") != run_id or saved.get("binding", {}).get("tool_ref") != tool_ref:
            raise PermissionError("Tool result no longer belongs to this run and binding")
        collection = next(
            ((path, items) for path, items in _lists(_structured_payload(saved.get("result")))
             if self._path_hash(path) == path_hash),
            None,
        )
        if collection is None or not 0 < offset < len(collection[1]):
            raise LookupError("This page of the original tool result is unavailable")
        path, items = collection
        page = collection_page(items, offset, _PAGE_ITEMS, page_character_budget())
        await authorize()
        end = offset + len(page)
        return {
            "path": list(path), "items": page, "offset": offset, "total": len(items),
            "result_cursor": self._cursor(context, run_id, tool_ref, invocation, path, end)
            if end < len(items) else None,
        }
