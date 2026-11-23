"""Space-scoped document maintenance using the same parsers and generators as a local vault."""
import asyncio
from contextlib import closing
from dataclasses import dataclass
from datetime import date
from time import monotonic

import sqlalchemy as sa

from ...vault_administration import _boolean
from ..document_exposure import saw_notes
from .derived import generate_llms_txt, generate_showcase
from .doctor import classify_violations, fix_note_content, FixResult
from .document_history import edits, lock_head, native_commit
from .documents import dependencies, folders, notes, moves, note_path, refresh_indexes
from .gate import run_gate
from .index import SyncStats, VaultIndex
from .parser import parse_note_text, rewrite_wikilinks
from .privacy import require_unprotected_execution
from .scaffold import scaffold_contents

_GENERATED = ("llms.txt", "_showcase/showcase.md")


@dataclass
class Plan:
    response: dict
    writes: dict
    sources: dict
    moves: dict
    folders_added: set
    folders_removed: set

    @classmethod
    def empty(cls):
        return cls({}, {}, {}, {}, set(), set())


def _checked_path(value, *, folder=False):
    if folder:
        if not isinstance(value, str):
            raise ValueError("A relative folder path is required")
        note_path(value + "/x.md")
        return value
    return note_path(value)


def _folder_names(paths):
    return {"/".join(parts[:i]) for path in paths
            for parts in (path.split("/"),) for i in range(1, len(parts))}


def _move(index, records, folder_markers, body, plan):
    old, new = body.get("from"), body.get("to")
    _checked_path(old, folder=True)
    _checked_path(new, folder=True)
    all_folders = folder_markers | _folder_names(records) | _folder_names(folder_markers)
    if old == new or new.startswith(old + "/"):
        raise ValueError("A folder cannot be moved inside itself")
    if new in records or new in all_folders or _folder_names([new]).intersection(records):
        plan.response = {"error": f"target already exists: {new}"}
        return
    if old in records:
        mapping = {old: _checked_path(new)}
    elif old in all_folders:
        mapping = {p: _checked_path(new + p[len(old):]) for p in records if p.startswith(old + "/")}
        plan.folders_removed = {p for p in folder_markers if p == old or p.startswith(old + "/")}
        plan.folders_added = {new + p[len(old):] for p in plan.folders_removed}
    else:
        plan.response = {"error": f"source not found: {old}"}
        return
    if set(_GENERATED).intersection((*mapping, *mapping.values())):
        raise ValueError("Generated outputs are owned by vault regeneration")
    updated = []
    for path, row in records.items():
        if path in _GENERATED:
            continue
        linked = set()
        def resolve(raw):
            resolved = index.resolve_link(raw)
            if resolved in mapping:
                linked.add(resolved)
            return resolved
        content, count = rewrite_wikilinks(row.content, resolve, mapping)
        target = mapping.get(path, path)
        if count:
            updated.append({"path": path, "links": count})
        if path in mapping:
            plan.writes[path] = None
            linked.add(path)
        if count or path in mapping:
            plan.writes[target] = content
            plan.sources[target] = {(p, records[p].revision) for p in linked if p != target}
    plan.moves = mapping
    plan.response = {"moved": {"from": old, "to": new}, "notes_moved": len(mapping),
                     "notes_updated": len(updated), "links_rewritten": sum(u["links"] for u in updated),
                     "updated": updated}


def _doctor(index, records, apply, plan):
    start = monotonic()
    before = run_gate(index)
    fix = FixResult(dry_run=not apply)
    for path, rules in classify_violations(before, fix).items():
        original = records[path].content
        content, applied = fix_note_content(original, parse_note_text(path, original), rules, date.today().isoformat())
        if applied:
            fix.fixed.append({"path": path, "fixes": applied})
        if apply and content != original:
            plan.writes[path] = content
    fix.files_changed = len(plan.writes)
    fix.elapsed_ms = int((monotonic() - start) * 1000)
    after = None
    if plan.writes:
        with closing(VaultIndex.from_documents((p, plan.writes.get(p, r.content))
                     for p, r in records.items() if p not in _GENERATED)) as repaired:
            after = run_gate(repaired).to_dict()
    plan.response = {"before": before.to_dict(), "fix": fix.to_dict(), "after": after}


def _plan(records, folder_markers, operation, query, body, existing_sources):
    plan = Plan.empty()
    if operation == "init":
        requested, documents = scaffold_contents()
        present_folders = folder_markers | _folder_names(records) | _folder_names(folder_markers)
        plan.folders_added = set(requested) - present_folders
        plan.writes = {p: content for p, content in documents.items() if p not in records}
        created = [p + "/" for p in requested if p in plan.folders_added] + list(plan.writes)
        plan.response = {"created": created, "count": len(created)}
        return plan
    with closing(VaultIndex.from_documents((p, r.content) for p, r in records.items()
                                            if p not in _GENERATED)) as index:
        if operation == "doctor":
            _doctor(index, records, _boolean(query.get("apply", False)), plan)
        elif operation == "move":
            _move(index, records, folder_markers, body, plan)
        elif operation == "derived":
            outputs = dict(zip(_GENERATED, (generate_llms_txt(index), generate_showcase(index))))
            sources = {(p, r.revision) for p, r in records.items() if p not in _GENERATED}
            for path, content in outputs.items():
                if path not in records or records[path].content != content or sources != existing_sources.get(path, set()):
                    plan.writes[path] = content
                    plan.sources[path] = sources
            plan.response = {"llms_txt": _GENERATED[0], "showcase": _GENERATED[1],
                             "llms_bytes": len(outputs[_GENERATED[0]].encode()),
                             "showcase_bytes": len(outputs[_GENERATED[1]].encode())}
        elif operation == "index/sync":
            stats = index.stats()
            force = _boolean(query.get("force", False))
            plan.response = SyncStats(total=stats["notes"], updated=stats["notes"] if force else 0,
                                      unchanged=0 if force else stats["notes"],
                                      links=stats["links"], broken=stats["broken_links"]).to_dict()
        else:
            raise ValueError("Unknown document maintenance operation")
    return plan


async def maintain(vault, context, operation, query, body, authorize, budget):
    start = monotonic()
    await require_unprotected_execution("Document vault administration")
    scope = await vault._scope(context, "memory.write")
    await vault._recheck(context, "memory.read", scope)
    async with vault.engine.begin() as con:
        head = await lock_head(con, scope)
        live = sa.select(notes).filter_by(**scope).where(notes.c.content.is_not(None))
        length = sa.func.octet_length(notes.c.content) if con.dialect.name == "postgresql" else sa.func.length(
            sa.cast(notes.c.content, sa.LargeBinary))
        count, size = (await con.execute(live.with_only_columns(sa.func.count(notes.c.id),
                                       sa.func.coalesce(sa.func.sum(length), 0)))).one()
        if count > budget[0] or size > budget[1]:
            raise ValueError("Document maintenance exceeds the configured space budget")
        records = {r.path: r for r in (await con.execute(live.order_by(notes.c.path))).mappings()}
        markers = set((await con.scalars(sa.select(folders.c.path).filter_by(**scope))).all())
        existing_sources = {}
        if operation == "derived":
            for path in _GENERATED:
                if path in records:
                    existing_sources[path] = set((await con.execute(sa.select(
                        dependencies.c.source_path, dependencies.c.source_revision).filter_by(
                            **scope, path=path, revision=records[path].revision))).all())
        plan = await asyncio.to_thread(_plan, records, markers, operation, query, body, existing_sources)
        if plan.folders_added:
            await con.execute(folders.insert(), [{**scope, "path": p} for p in sorted(plan.folders_added)])
        if plan.folders_removed:
            await con.execute(folders.delete().filter_by(**scope).where(folders.c.path.in_(plan.folders_removed)))
        revisions = {}
        for path, content in plan.writes.items():
            if content is not None and len(content.encode()) > 2 << 20:
                raise ValueError("Generated note exceeds document size limit")
            previous = records[path].revision if path in records else await con.scalar(
                sa.select(notes.c.revision).filter_by(**scope, path=path))
            revision = await vault._apply(con, context, scope, path, content, previous or 0,
                                          historical=operation == "move", regenerate=operation == "derived")
            revisions[path] = revision
            sources = plan.sources.get(path)
            if sources:
                await con.execute(dependencies.insert(), [
                    {**scope, "path": path, "revision": revision, "source_path": p, "source_revision": rev}
                    for p, rev in sorted(sources)])
        for source, target in plan.moves.items():
            last_delete = await con.scalar(sa.select(sa.func.max(edits.c.revision)).filter_by(
                **scope, path=source, present=False))
            await con.execute(moves.insert().values(**scope, path=source, first_revision=(last_delete or 0)+1,
                last_revision=records[source].revision, destination=target, destination_revision=revisions[target]))
        if operation == "index/sync":
            if _boolean(query.get("force", False)):
                for path, row in records.items():
                    parsed = parse_note_text(path, row.content)
                    await refresh_indexes(con, {**scope, "path": path}, row.id, parsed, row.content)
            plan.response["elapsed_ms"] = int((monotonic() - start)*1000)
        elif "error" not in plan.response:
            changes = [(path, rev, plan.writes[path] is not None) for path, rev in revisions.items()]
            plan.response["commit"] = await native_commit(con, scope, head, context, changes,
                                    f"Vault {operation}") if changes else None
        await vault._recheck(context, "memory.read", scope)
        await vault._recheck(context, "memory.write", scope)
        await authorize()
    await saw_notes(vault, context, scope, list(records.values()))
    await vault._recheck(context, "memory.read", scope)
    return plan.response
