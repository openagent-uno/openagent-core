"""Management operations over the same authorized document service used by agents."""
from ...vault_administration import _boolean, _json, _limit, _search_lines
from .model import GateConfig
from .parser import extract_wikilinks, load_frontmatter_yaml, split_frontmatter


class DocumentVaultAdministration:
    def __init__(self, store, authorizer, execution):
        self.store = store
        self.authorizer = authorizer
        self.execution = execution

    async def authorize(self, context, action):
        from ...contracts import ResourceRef, require_authorized
        if context.authority != self.execution.authority or context.agent_id != self.execution.agent_id:
            raise PermissionError("Different vault authority")
        target = ResourceRef("agent", context.tenant_id, context.agent_id)
        await require_authorized(self.authorizer, context, action, target)

    async def execute(self, context, operation, *, path=None, query=None, body=None):
        query, body = query or {}, body or {}
        readable = {"notes", "read", "graph", "stats", "gate", "search", "search/files", "search/in-file"}
        if operation not in readable | {"write", "delete"}:
            raise NotImplementedError("This operation requires version-history administration")
        action = "vault.read" if operation in readable else "vault.write"
        await self.authorize(context, action)
        result = await self._execute(operation, path, query, body)
        await self.authorize(context, action)
        return result

    async def _execute(self, operation, path, query, body):
        execution = self.execution
        if operation in {"read", "search/in-file"}:
            row = await self.store.read(execution, path if operation == "read" else query["path"])
            if operation == "search/in-file":
                needle = query.get("q")
                if not isinstance(needle, str) or not 1 <= len(needle) <= 4096:
                    raise ValueError("A bounded query is required")
                regex = _boolean(query.get("regex", False))
                hits = await _search_lines(row["content"], needle, regex)
                return {"path": row["path"], "query": needle, "regex": regex, "matches": hits, "count": len(hits)}
            raw, body_text = split_frontmatter(row["content"])
            return {**row, "frontmatter": _json(load_frontmatter_yaml(raw)) if raw else {},
                    "body": body_text, "links": [target for target, _ in extract_wikilinks(row["content"])]}
        if operation in {"write", "delete"}:
            expected = body.get("expected_revision")
            if expected is None:
                try:
                    expected = (await self.store.read(execution, path))["revision"]
                except LookupError:
                    if operation == "delete":
                        raise
                    expected = 0
            if operation == "delete":
                result = await self.store.delete(execution, path, expected_revision=expected)
            else:
                result = await self.store.write(execution, path, body["content"], expected_revision=expected)
            return {"ok": True, "path": path, "revision": result["revision"],
                    "existed": expected != 0, "warnings": [], "applied": []}
        if operation == "notes":
            page, rows = None, []
            while True:
                result = await self.store.list_notes(execution, after=page, limit=200)
                rows.extend(result["notes"])
                page = result["next_cursor"]
                if page is None:
                    return {"notes": rows}
        if operation in {"search", "search/files"}:
            text = query.get("q", "").strip()
            rows = await self.store.search(execution, query=text, limit=_limit(query.get("limit"), 50, 200)) if text else {"hits": []}
            return {"results": rows["hits"]}
        result = await self.store.inspect_corpus(execution, operation,
                     config=GateConfig(strict=_boolean(query.get("strict", False))))
        if operation == "gate":
            limit = _limit(query.get("limit"), 500, 10000)
            if len(result["violations"]) > limit:
                result["violations"] = result["violations"][:limit]
                result["violations_truncated"] = True
        return result
