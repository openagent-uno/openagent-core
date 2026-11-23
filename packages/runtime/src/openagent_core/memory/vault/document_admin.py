"""Management operations over the same authorized document service used by agents."""
from ...vault_administration import _boolean, _json, _limit, _search_lines
from .model import GateConfig
from .parser import FrontmatterSyntaxError
from .parser import extract_wikilinks, load_frontmatter_yaml, split_frontmatter


class DocumentVaultAdministration:
    def __init__(self, store, authorizer, execution, *, validate_on_write=False, max_maintenance_bytes=64 << 20, max_maintenance_notes=100_000):
        if min(max_maintenance_bytes, max_maintenance_notes) < 1:
            raise ValueError("Invalid maintenance budget")
        self.validate_on_write = validate_on_write
        self.maintenance_budget = max_maintenance_notes, max_maintenance_bytes
        self.store = store
        self.authorizer = authorizer
        self.execution = execution

    async def authorize(self, context, action):
        from ...administration import ManagementContext
        from ...contracts import ResourceRef, require_authorized
        if not isinstance(context, ManagementContext):
            raise TypeError("A verified management context is required")
        if context.authority != self.execution.authority or context.agent_id != self.execution.agent_id:
            raise PermissionError("Different vault authority")
        target = ResourceRef("agent", context.tenant_id, context.agent_id)
        await require_authorized(self.authorizer, context, action, target)

    async def execute(self, context, operation, *, path=None, query=None, body=None):
        query = {} if query is None else query
        body = {} if body is None else body
        if not isinstance(query, dict) or not isinstance(body, dict):
            raise ValueError("Expected object")
        allowed_query = {"notes": {"after", "limit"}, "search": {"q", "limit"}, "search/files": {"q", "limit"},
            "search/in-file": {"path", "q", "regex"}, "gate": {"strict", "limit"},
            "history": {"path", "limit"}, "commit": {"hash"}, "doctor": {"apply"}, "index/sync": {"force"}}
        allowed_body = {"write": {"content","expected_revision"}, "delete": {"expected_revision"},
                        "restore": {"hash"}, "reset": {"hash","confirm"}, "move": {"from","to"}}
        if set(query) - allowed_query.get(operation, set()) or set(body) - allowed_body.get(operation, set()):
            raise ValueError("Unknown vault fields")
        if path is not None and operation not in {"read","write","delete"}:
            raise ValueError("Unexpected note path")
        readable = {"notes", "read", "graph", "stats", "gate", "search", "search/files", "search/in-file","history","commit"}
        if operation not in readable | {"write", "delete", "restore", "reset", "move", "doctor", "derived", "init", "index/sync"}:
            raise NotImplementedError("This operation requires version-history administration")
        action = "vault.read" if operation in readable else "vault.write"
        await self.authorize(context, action)
        if operation in {"restore","reset"}:
            from .document_history import restore
            if operation == "reset" and body.get("confirm") is not True:
                raise ValueError("An explicit reset confirmation is required")
            result = await restore(self.store,self.execution,body.get("hash"),reset=operation=="reset",
                                   authorize=lambda:self.authorize(context,action))
        else:
            result = await self._execute(operation, path, query, body, lambda:self.authorize(context,action))
        await self.authorize(context, action)
        return result

    async def _execute(self, operation, path, query, body, before_commit):
        execution = self.execution
        if operation in {"move", "doctor", "derived", "init", "index/sync"}:
            from .document_maintenance import maintain
            return await maintain(self.store, execution, operation, query, body, before_commit, self.maintenance_budget)
        if operation in {"history","commit"}:
            from . import document_history
            if operation == "history":
                return await document_history.history(self.store,execution,path=query.get("path"),limit=_limit(query.get("limit"),50,200))
            return await document_history.detail(self.store,execution,query.get("hash"))
        if operation in {"read", "search/in-file"}:
            if operation == "search/in-file":
                row = await self.store.read(execution, query["path"])
                needle = query.get("q")
                if not isinstance(needle, str) or not 1 <= len(needle) <= 4096:
                    raise ValueError("A bounded query is required")
                regex = _boolean(query.get("regex", False))
                hits = await _search_lines(row["content"], needle, regex)
                return {"path": row["path"], "query": needle, "regex": regex, "matches": hits, "count": len(hits)}
            from .document_browser import browse
            from .documents import note_path
            page = await browse(self.store, execution, path=note_path(path, allow_generated=True), limit=1, include_content=True)
            if not page["notes"]:
                raise LookupError("Note not found")
            row = page["notes"][0]
            raw, body_text = split_frontmatter(row["content"])
            try:
                meta = load_frontmatter_yaml(raw) if raw else {}
            except FrontmatterSyntaxError:
                meta = {}
            return {**row, "frontmatter": _json(meta) if isinstance(meta, dict) else {},
                    "body": body_text.strip() if raw is not None else row["content"], "links": [target for target, _ in extract_wikilinks(row["content"])]}
        if operation in {"write", "delete"}:
            expected = body.get("expected_revision")
            if expected is None:
                try:
                    expected = (await self.store.read(execution, path))["revision"]
                except LookupError:
                    if operation == "delete":
                        raise
                    expected = 0
            warnings, applied = [], []
            if operation == "delete":
                result = await self.store.delete(execution, path, expected_revision=expected,before_commit=before_commit)
            else:
                from . import taxonomy
                from .documents import note_path
                path = note_path(path)
                content = body.get("content")
                if not isinstance(content, str) or len(content.encode()) > 2 << 20:
                    raise ValueError("Invalid note content")
                config = GateConfig()
                if self.validate_on_write and taxonomy.is_in_quality_scope(
                        path, config.excluded_folders, config.raw_prefixes, config.journal_root):
                    import asyncio
                    from .edited_note import validate_edited_note
                    content, errors, warnings, applied = await asyncio.to_thread(
                        validate_edited_note, path, content, is_new=expected == 0, config=config)
                    if errors:
                        return {"ok": False, "blocked": True, "existed": expected != 0,
                                "warnings": warnings, "errors": errors}
                result = await self.store.write(execution, path, content, expected_revision=expected,before_commit=before_commit)
            return {"ok": True, "path": path, "revision": result["revision"],
                    "existed": expected != 0, "warnings": warnings, "applied": applied, "commit":result.get("commit")}
        if operation == "notes":
            from .document_browser import browse
            if query:
                return await browse(self.store, execution, after=query.get("after"), limit=_limit(query.get("limit"), 50, 200))
            page, rows = None, []
            while True:
                result = await browse(self.store, execution, after=page, limit=200)
                rows.extend(result["notes"])
                page = result["next_cursor"]
                if page is None:
                    return {"notes": rows}
        if operation in {"search", "search/files"}:
            text = query.get("q", "").strip()
            limit = _limit(query.get("limit"), 50, 200)
            if operation == "search/files" and text:
                from .document_browser import browse
                page = await browse(self.store, execution, query=text, limit=limit)
                return {"results": [{k: row[k] for k in ("path", "title", "tags")} for row in page["notes"]]}
            rows = await self.store.search(execution, query=text, limit=limit) if text else {"hits": []}
            return {"results": rows["hits"]}
        result = await self.store.inspect_corpus(execution, operation,
                     config=GateConfig(strict=_boolean(query.get("strict", False))))
        if operation == "gate":
            limit = _limit(query.get("limit"), 500, 10000)
            if len(result["violations"]) > limit:
                result["violations"] = result["violations"][:limit]
                result["violations_truncated"] = True
        return result
