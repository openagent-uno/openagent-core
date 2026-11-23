"""Versioned Markdown notes for hosts that supply an asynchronous SQL engine."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import PurePosixPath

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from openagent_core.contracts import ExecutionContext, ResourceRef, require_authorized
from openagent_core.memory.vault.parser import link_key, parse_note_text
from openagent_core.memory.vault.privacy import require_unprotected_execution
from openagent_core.runtime import current_run_id, current_runtime
from .document_dream import DocumentDreamQueue, check_dream_space, is_document_dream


class RevisionConflict(ValueError):
    """The writer's observed version is no longer current."""


metadata = sa.MetaData()


def identity_columns(*, primary_key=True):
    return (
        sa.Column("tenant", sa.String(200), primary_key=primary_key, nullable=False),
        sa.Column("agent", sa.String(200), primary_key=primary_key, nullable=False),
        sa.Column("space", sa.String(200), primary_key=primary_key, nullable=False),
        sa.Column("path", sa.String(700), primary_key=primary_key, nullable=False),
    )


notes = sa.Table("oa_vault_notes", metadata,
    sa.Column("id", sa.BigInteger().with_variant(sa.Integer, "sqlite"), primary_key=True),
    *identity_columns(primary_key=False),
    sa.UniqueConstraint("tenant", "agent", "space", "path"),
    sa.Column("revision", sa.BigInteger, nullable=False),
    sa.Column("title", sa.Text), sa.Column("content", sa.Text),
)
versions = sa.Table("oa_vault_revisions", metadata, *identity_columns(),
    sa.Column("revision", sa.BigInteger, primary_key=True),
    sa.Column("content", sa.Text, nullable=False),
    sa.Column("author", sa.Text, nullable=False),
    sa.Column("session_id", sa.Text, nullable=False),
    sa.Column("run_id", sa.Text),
)
links = sa.Table("oa_vault_links", metadata, *identity_columns(),
    sa.Column("target", sa.String(700), primary_key=True),
)
sa.Index("oa_vault_backlinks", links.c.tenant, links.c.agent, links.c.space, links.c.target)


# Imported provenance is historical evidence, independent of runtime principals.
import_origins = sa.Table("oa_vault_import_origins", metadata, *identity_columns(),
    sa.Column("revision", sa.BigInteger, primary_key=True),
    sa.Column("facts", sa.Text, nullable=False),
)


async def refresh_indexes(con, key, note_id, parsed, content):
    await con.execute(sa.delete(links).filter_by(**key))
    if parsed:
        targets = sorted({link_key(link) for link in parsed.outlinks if len(link_key(link).encode()) <= 700})
        if targets:
            await con.execute(sa.insert(links), [{**key, "target": target} for target in targets])
    if con.dialect.name == "sqlite":
        await con.execute(sa.text("DELETE FROM oa_vault_search WHERE rowid=:id"), {"id": note_id})
        if parsed:
            await con.execute(sa.text("INSERT INTO oa_vault_search(rowid,content) VALUES (:id,:content)"),
                              {"id": note_id, "content": content})


def note_path(value: str) -> str:
    if (not isinstance(value, str) or len(value.encode()) > 700 or "\\" in value
            or ":" in value or "\0" in value or PurePosixPath(value).suffix.lower() != ".md"
            or any(not part or part.startswith(".") for part in value.split("/"))):
        raise ValueError("Expected a relative Markdown note path")
    return value


class SqlDocumentVault:
    """A space is one ACL domain. The host selects it from verified context.

    All notes in a space must be readable by its authorized readers. Assign
    distinct spaces to private and shared corpora. The host owns the engine.
    """

    def __init__(self, engine: AsyncEngine, authorizer,
                 resolve_space: Callable[[ExecutionContext], Awaitable[str]], *, dream_queue: DocumentDreamQueue | None = None):
        if engine.dialect.name not in {"sqlite", "postgresql"}:
            raise ValueError("Document vault requires SQLite or PostgreSQL")
        self.engine = engine
        self.authorizer = authorizer
        self.resolve_space = resolve_space
        if dream_queue is not None and dream_queue.engine is not engine:
            raise ValueError("Document dreams must share the vault transaction engine")
        self.dream_queue = dream_queue

    async def initialize(self):
        """Install the schema during host provisioning, before starting workers."""
        if self.dream_queue is not None:
            await self.dream_queue.initialize()
        async with self.engine.begin() as connection:
            from .document_history import heads, lock_head, meta as history_schema
            await connection.run_sync(metadata.create_all)
            await connection.run_sync(history_schema.create_all)
            old_spaces = (await connection.execute(sa.select(notes.c.tenant,notes.c.agent,notes.c.space)
                .where(~sa.exists(sa.select(heads.c.head).where(heads.c.tenant==notes.c.tenant,
                         heads.c.agent==notes.c.agent,heads.c.space==notes.c.space))).distinct())).mappings().all()
            for fields in old_spaces:
                await lock_head(connection,dict(fields))
            if self.engine.dialect.name == "postgresql":
                await connection.execute(sa.text("""
                    CREATE INDEX IF NOT EXISTS oa_vault_search ON oa_vault_notes
                    USING GIN (to_tsvector('simple', content)) WHERE content IS NOT NULL
                """))
            else:
                await connection.execute(sa.text("""
                    CREATE VIRTUAL TABLE IF NOT EXISTS oa_vault_search USING fts5(
                        content
                    )
                """))

    async def _scope(self, context, action):
        if not isinstance(context, ExecutionContext):
            raise TypeError("A verified execution context is required")
        space = await self.resolve_space(context)
        fields = {"tenant": context.tenant_id, "agent": context.agent_id, "space": space}
        check_dream_space(context, fields)
        if any(not isinstance(v, str) or not v or len(v.encode()) > 200 for v in fields.values()):
            raise ValueError("Invalid vault space identity")
        target = ResourceRef("vault-space", context.tenant_id, space)
        if is_document_dream(context):
            runtime = current_runtime()
            if runtime is None or not current_run_id() or not context.deferred or not context.delegation_id:
                raise PermissionError("A live delegated dream run is required")
            await runtime.authorize(context, "memory.dream", target, audience=context.audience)
        await require_authorized(self.authorizer, context, action, target, audience=(context.initiator,))
        await require_authorized(self.authorizer, context, "memory.publish", target, audience=context.audience)
        return fields

    async def _recheck(self, context, action, scope):
        if await self._scope(context, action) != scope:
            raise PermissionError("The selected vault space changed")

    async def read(self, context, path, *, revision=None):
        scope = await self._scope(context, "memory.read")
        async with self.engine.connect() as con:
            stmt = sa.select(notes.c.path, notes.c.title, notes.c.content, notes.c.revision).filter_by(
                **scope, path=note_path(path)).where(notes.c.content.is_not(None))
            if revision is not None:
                if type(revision) is not int or not 1 <= revision < (1 << 63):
                    raise ValueError("Invalid note revision")
                stmt = sa.select(versions.c.path, versions.c.content, versions.c.revision).filter_by(
                    **scope, path=path, revision=revision)
            row = (await con.execute(stmt)).mappings().one_or_none()
        await self._recheck(context, "memory.read", scope)
        if row is None:
            raise LookupError("Note not found")
        return dict(row)

    async def list_notes(self, context, *, after=None, limit=50):
        scope = await self._scope(context, "memory.read")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("Invalid vault page size")
        stmt = sa.select(notes.c.path, notes.c.title, notes.c.revision).filter_by(**scope).where(
            notes.c.content.is_not(None))
        if after is not None:
            stmt = stmt.where(notes.c.path > note_path(after))
        async with self.engine.connect() as con:
            rows = (await con.execute(stmt.order_by(notes.c.path).limit(limit + 1))).mappings().all()
        await self._recheck(context, "memory.read", scope)
        return {"notes": [dict(row) for row in rows[:limit]],
                "next_cursor": rows[limit - 1]["path"] if len(rows) > limit else None}

    async def search(self, context, *, query, limit=20):
        return await self._search(context, query=query, limit=limit, paginate=False, after=None)

    async def search_page(self, context, *, query, limit=20, after=None):
        """Keyset pagination of full-text matches in stable path order."""
        return await self._search(context, query=query, limit=limit, paginate=True, after=after)

    async def _search(self, context, *, query, limit, paginate, after):
        scope = await self._scope(context, "memory.read")
        if not isinstance(query, str) or not query.strip() or len(query) > 1024:
            raise ValueError("Invalid vault search")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("Invalid vault page size")
        if self.engine.dialect.name == "postgresql":
            stmt = """
                SELECT path,title,revision FROM oa_vault_notes
                WHERE tenant=:tenant AND agent=:agent AND space=:space AND content IS NOT NULL
                AND to_tsvector('simple',content) @@ plainto_tsquery('simple',:query)
            """
            path = "path"
            rank = "ts_rank(to_tsvector('simple',content),plainto_tsquery('simple',:query)) DESC"
        else:
            # Treat user input as terms, independent of FTS5 query syntax.
            query = " AND ".join('"' + word.replace('"', '""') + '"' for word in query.split())
            stmt = """
                SELECT n.path,n.title,n.revision FROM oa_vault_search s
                JOIN oa_vault_notes n ON n.id = s.rowid
                WHERE oa_vault_search MATCH :query AND n.tenant=:tenant AND n.agent=:agent
                AND n.space=:space AND n.content IS NOT NULL
            """
            path, rank = "n.path", "rank"
        params = {**scope, "query": query, "limit": limit + int(paginate)}
        if after is not None:
            params["after"] = note_path(after)
            stmt += f" AND {path} > :after"
        order = path if paginate else f"{rank}, {path}"
        stmt += f" ORDER BY {order} LIMIT :limit"
        async with self.engine.connect() as con:
            rows = (await con.execute(sa.text(stmt), params)).mappings().all()
        await self._recheck(context, "memory.read", scope)
        result = {"hits": [{"kind": "note", **dict(row)} for row in rows[:limit]]}
        if paginate:
            result["next_cursor"] = rows[limit - 1]["path"] if len(rows) > limit else None
        return result

    async def inspect_corpus(self, context, operation, *, config=None):
        """Execute an explicit whole-space graph or quality inspection."""
        import asyncio
        from .gate import run_gate
        from .index import VaultIndex

        if operation not in {"graph", "gate", "stats"}:
            raise ValueError("Unknown vault inspection")
        scope = await self._scope(context, "memory.read")
        async with self.engine.connect() as con:
            records = (await con.execute(sa.select(notes.c.path, notes.c.content)
                       .filter_by(**scope).where(notes.c.content.is_not(None)))).all()

        def inspect():
            index = VaultIndex.from_documents(records)
            try:
                if operation == "gate":
                    return run_gate(index, config).to_dict()
                if operation == "stats":
                    return index.stats()
                nodes, edges = [], []
                for note in index.all_notes():
                    nodes.append({"id": note.path, "label": note.title or note.stem, "tags": note.tags})
                    targets = {index.resolve_link(link) for link in note.outlinks}
                    edges.extend({"source": note.path, "target": target} for target in sorted(targets - {None, note.path}))
                return {"nodes": nodes, "edges": edges}
            finally:
                index.close()

        result = await asyncio.to_thread(inspect)
        await self._recheck(context, "memory.read", scope)
        return result

    async def backlinks(self, context, path, *, after=None, limit=50):
        """Page distinct sources that link to this vault-relative path."""
        target = link_key(note_path(path))
        scope = await self._scope(context, "memory.read")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("Invalid vault page size")
        stmt = sa.select(links.c.path).filter_by(**scope, target=target)
        if after is not None:
            stmt = stmt.where(links.c.path > note_path(after))
        async with self.engine.connect() as con:
            rows = (await con.execute(stmt.order_by(links.c.path).limit(limit + 1))).scalars().all()
        await self._recheck(context, "memory.read", scope)
        return {"paths": list(rows[:limit]), "next_cursor": rows[limit - 1] if len(rows) > limit else None}

    async def history(self, context, path, *, before=None, limit=50):
        """Return revision metadata, newest first, for this space only."""
        scope = await self._scope(context, "memory.read")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("Invalid vault page size")
        stmt = sa.select(versions.c.revision).filter_by(**scope, path=note_path(path))
        if before is not None:
            if type(before) is not int or before < 1:
                raise ValueError("Invalid revision cursor")
            stmt = stmt.where(versions.c.revision < before)
        async with self.engine.connect() as con:
            rows = (await con.execute(stmt.order_by(versions.c.revision.desc()).limit(limit + 1))).scalars().all()
        await self._recheck(context, "memory.read", scope)
        return {"revisions": list(rows[:limit]), "next_cursor": rows[limit - 1] if len(rows) > limit else None}

    async def revision_origin(self, context, path, *, revision):
        """Return the separately attributed source evidence of an imported revision."""
        import json
        if type(revision) is not int or revision < 1:
            raise ValueError("Invalid note revision")
        scope = await self._scope(context, "memory.read")
        async with self.engine.connect() as con:
            facts = (await con.execute(sa.select(import_origins.c.facts).filter_by(
                **scope, path=note_path(path), revision=revision))).scalar_one_or_none()
        await self._recheck(context, "memory.read", scope)
        return json.loads(facts) if facts is not None else None

    async def write(self, context, path, content, *, expected_revision, before_commit=None):
        if not isinstance(content, str) or len(content.encode()) > 2 << 20 or "\0" in content:
            raise ValueError("Invalid Markdown content")
        return await self._change(context, note_path(path), content, expected_revision, before_commit=before_commit)

    async def delete(self, context, path, *, expected_revision, before_commit=None):
        """Remove a note from the current corpus while retaining its revisions."""
        if expected_revision == 0:
            raise ValueError("Deleting a note requires an existing revision")
        return await self._change(context, note_path(path), None, expected_revision, before_commit=before_commit)

    async def forget(self, context, path, *, expected_revision):
        if expected_revision == 0:
            raise ValueError("Forgetting a note requires an existing revision")
        return await self._change(context, note_path(path), None, expected_revision, erase=True)

    async def _change(self, context, path, content, expected, *, erase=False, before_commit=None):
        await require_unprotected_execution("Document vault")
        if type(expected) is not int or expected < 0 or expected >= (1 << 63) - 1:
            raise ValueError("Invalid expected revision")
        scope = await self._scope(context, "memory.write")
        from .document_history import lock_head, native_commit, redact
        async with self.engine.begin() as con:
            head = await lock_head(con, scope)
            revision = await self._apply(con,context,scope,path,content,expected,erase=erase)
            if erase:
                await redact(con,scope,path)
                digest = None
            else:
                digest = await native_commit(con,scope,head,context,[(path,revision,content is not None)],"Document update")
            await self._recheck(context, "memory.write", scope)
            if before_commit is not None:
                await before_commit()
        return {"path":path,"revision":revision,"forgotten":erase,"commit":digest}

    async def _apply(self,con,context,scope,path,content,expected,*,erase=False,historical=False):
        key = {**scope,"path":path}
        parsed = parse_note_text(path, content) if content is not None else None
        if parsed and not parsed.frontmatter_valid and not historical:
            raise ValueError("Invalid Markdown frontmatter")
        revision = expected + 1
        values = {"content":content, "title":parsed.title if parsed else None, "revision":revision}
        if expected == 0:
            # ON CONFLICT is limited to this note's identity. Other database
            # errors still reach the caller and roll back the transaction.
            if self.engine.dialect.name == "postgresql":
                from sqlalchemy.dialects.postgresql import insert
            else:
                from sqlalchemy.dialects.sqlite import insert
            stmt = insert(notes).values(**key, **values).on_conflict_do_nothing(
                index_elements=list(key))
        else:
            mutable = notes.c.content.is_not(None)
            if content is not None or erase:
                saved = sa.select(versions.c.revision).filter_by(**key).exists()
                origin = sa.select(import_origins.c.revision).filter_by(**key).exists()
                mutable = sa.or_(mutable, saved, origin)
            stmt = sa.update(notes).filter_by(**key, revision=expected).where(mutable).values(**values)
        changed = (await con.execute(stmt.returning(notes.c.id))).scalar_one_or_none()
        if changed is None:
            raise RevisionConflict("Note has a conflicting revision or was forgotten")
        if parsed:
            await con.execute(sa.insert(versions).values(**key, revision=revision, content=content,
                author=context.author.key, session_id=context.session_id, run_id=current_run_id()))
        elif erase:
            await con.execute(sa.delete(versions).filter_by(**key))
            await con.execute(sa.delete(import_origins).filter_by(**key))
        await refresh_indexes(con, key, changed, parsed, content)
        if is_document_dream(context):
            if self.dream_queue is None:
                raise PermissionError("Document maintenance is disabled")
            await self.dream_queue.verify_dream_write(con, scope, current_run_id())
        elif self.dream_queue is not None:
            await self.dream_queue.mark_changed(con, scope)
        return revision
