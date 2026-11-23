"""Bounded metadata and filename projections for authorized note editors."""
from datetime import datetime

import sqlalchemy as sa

from ..document_exposure import saw_notes
from .document_history import commits, edits
from .documents import note_path, notes, properties
from .parser import parse_note_text


async def browse(vault, context, *, limit, after=None, path=None, query=None, include_content=False):
    scope = await vault._scope(context, "memory.read")
    if type(limit) is not int or not 1 <= limit <= 200:
        raise ValueError("Invalid vault page size")
    # Imported snapshots may lack an original timestamp. History without a date
    # is displayed as unknown. A restore can introduce a new head for old bytes.
    source_date = (sa.select(commits.c.date).select_from(edits.join(commits, sa.and_(
        *(edits.c[k] == commits.c[k] for k in (*scope, "digest")))))
        .where(*(edits.c[k] == notes.c[k] for k in (*scope, "path", "revision")))
        .order_by(commits.c.serial.desc()).limit(1).correlate(notes).scalar_subquery())
    body = notes.c.content if include_content else sa.case(
        (properties.c.note_id.is_(None), notes.c.content), else_=None)
    stmt = (sa.select(notes.c.path, notes.c.title, notes.c.revision, properties.c.tags, properties.c.note_type,
                      properties.c.size_bytes, source_date.label("date"), body.label("body"))
            .select_from(notes.outerjoin(properties, sa.and_(notes.c.id == properties.c.note_id,
                         notes.c.revision == properties.c.revision))).where(
                *(notes.c[k] == v for k, v in scope.items()), notes.c.content.is_not(None)))
    if after is not None:
        stmt = stmt.where(notes.c.path > note_path(after, allow_generated=True))
    if path is not None:
        stmt = stmt.where(notes.c.path == note_path(path, allow_generated=True))
    if query is not None:
        if not isinstance(query, str) or not query.strip() or len(query) > 1024:
            raise ValueError("Invalid vault filename query")
        stmt = stmt.where(sa.func.lower(notes.c.path).contains(query.strip().lower(), autoescape=True))
    async with vault.engine.connect() as con:
        page = (await con.execute(stmt.order_by(notes.c.path).limit(limit + 1))).mappings().all()
    await vault._recheck(context, "memory.read", scope)
    await saw_notes(vault, context, scope, page[:limit])
    result = []
    for row in page[:limit]:
        tags, kind, size = row["tags"], row["note_type"], row["size_bytes"]
        if size is None:
            # A database provisioned before this index can be used immediately.
            parsed = parse_note_text(row["path"], row["body"])
            tags, kind, size = parsed.tags, str(parsed.frontmatter.get("type") or ""), parsed.byte_size
        stamp = datetime.fromisoformat(row["date"]) if row["date"] else None
        item = {"path": row["path"], "title": row["title"], "revision": row["revision"], "tags": tags,
                "type": kind, "size": size, "modified": stamp.timestamp() if stamp and stamp.tzinfo else None}
        if include_content:
            item["content"] = row["body"]
        result.append(item)
    return {"notes": result, "next_cursor": result[-1]["path"] if len(page) > limit else None}
