"""Scoped SQL revision journal and desktop history projection."""

from datetime import datetime, timezone
import difflib
from hashlib import sha256
import json
import re
from uuid import uuid4

import sqlalchemy as sa

from ...contracts import canonical_json
from ...runtime import current_run_id
from .documents import identity_columns, notes, versions, note_path

meta = sa.MetaData()


def scope_columns():
    return identity_columns()[:3]


heads = sa.Table(
    "oa_vault_history_heads",
    meta,
    *scope_columns(),
    sa.Column("head", sa.String(64)),
    sa.Column("serial", sa.BigInteger, nullable=False),
)
commits = sa.Table(
    "oa_vault_history_commits",
    meta,
    *scope_columns(),
    sa.Column("digest", sa.String(64), primary_key=True),
    sa.Column("serial", sa.BigInteger, nullable=False),
    sa.Column("parent", sa.String(64)),
    sa.Column("source", sa.Text, nullable=False),
    sa.Column("date", sa.Text, nullable=False),
    sa.Column("author", sa.Text, nullable=False),
    sa.Column("provenance", sa.Text, nullable=False),
    sa.Column("subject", sa.Text, nullable=False),
)
edits = sa.Table(
    "oa_vault_history_edits",
    meta,
    *scope_columns(),
    sa.Column("digest", sa.String(64), primary_key=True),
    sa.Column("path", sa.String(700), primary_key=True),
    sa.Column("revision", sa.BigInteger, nullable=False),
    sa.Column("present", sa.Boolean, nullable=False),
)
sa.Index(
    "oa_vault_log_page",
    commits.c.tenant,
    commits.c.agent,
    commits.c.space,
    commits.c.serial,
)
sa.Index(
    "oa_vault_log_path",
    edits.c.tenant,
    edits.c.agent,
    edits.c.space,
    edits.c.path,
    edits.c.revision,
)


def _now():
    return datetime.now(timezone.utc).isoformat()


async def _insert(
    con, scope, digest, serial, parent, source, date, author, provenance, subject
):
    await con.execute(
        sa.insert(commits).values(
            **scope,
            digest=digest,
            serial=serial,
            parent=parent,
            source=source,
            date=date,
            author=author,
            provenance=canonical_json(provenance),
            subject=subject,
        )
    )


async def edit(con, scope, digest, path, revision, present):
    await con.execute(
        sa.insert(edits).values(
            **scope, digest=digest, path=path, revision=revision, present=present
        )
    )


async def _advance(con, scope, digest, serial):
    await con.execute(
        sa.update(heads).filter_by(**scope).values(head=digest, serial=serial)
    )


async def lock_head(con, scope):
    if con.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    await con.execute(
        insert(heads)
        .values(**scope, serial=0)
        .on_conflict_do_nothing(index_elements=list(scope))
    )
    row = (
        await con.execute(
            sa.update(heads)
            .filter_by(**scope)
            .values(serial=heads.c.serial)
            .returning(heads.c.head, heads.c.serial)
        )
    ).one()
    if row.head is not None:
        return tuple(row)
    prior = (
        await con.execute(
            sa.select(notes.c.path, notes.c.revision)
            .filter_by(**scope)
            .where(notes.c.content.is_not(None))
        )
    ).all()
    if not prior:
        return None, 0
    digest = uuid4().hex
    await _insert(
        con,
        scope,
        digest,
        1,
        None,
        "baseline",
        _now(),
        "",
        {"origin": "baseline"},
        "Existing document space",
    )
    await con.execute(
        sa.insert(edits),
        [
            {**scope, "digest": digest, "path": p, "revision": r, "present": True}
            for p, r in prior
        ],
    )
    await _advance(con, scope, digest, 1)
    return digest, 1


async def native_commit(con, scope, head, context, changes, subject, *, restored=None):
    digest = uuid4().hex
    origin = {"origin": "runtime", "session": context.session_id}
    if current_run_id():
        origin["run"] = current_run_id()
    if restored:
        origin["restored_from"] = restored
    await _insert(
        con,
        scope,
        digest,
        head[1] + 1,
        head[0],
        "native",
        _now(),
        context.author.key,
        origin,
        subject,
    )
    for path, rev, present in changes:
        await edit(con, scope, digest, path, rev, present)
    await _advance(con, scope, digest, head[1] + 1)
    return digest


async def seed_import(con, scope, src):
    """Publish original commit identities and sparse changes in the same transaction as their notes."""
    from .documents import import_origins

    baseline = sha256(
        canonical_json([src.receipt, src.corpus, scope, "working-tree"]).encode()
    ).hexdigest()
    await con.execute(
        sa.insert(heads).values(**scope, head=baseline, serial=len(src.graph) + 1)
    )
    for digest, (parent, facts) in src.graph.items():
        # The repository's original commit message can describe unselected files.
        # Scoped clients receive its author and approved provenance plus a neutral subject.
        author, clock, offset = facts["author"].rsplit(" ", 2)
        stamp = datetime.fromtimestamp(int(clock), timezone.utc).isoformat()
        origin = {k.lower(): v for k, v in facts["origin"]}
        origin.setdefault("origin", "git")
        await _insert(
            con,
            scope,
            digest,
            facts["order"],
            parent,
            "git",
            stamp,
            author,
            origin,
            "Document revision",
        )
    await _insert(
        con,
        scope,
        baseline,
        len(src.graph) + 1,
        src.active_parent,
        "working_tree",
        "",
        "",
        {"origin": "snapshot"},
        "Captured working tree",
    )
    result = await con.stream(
        sa.select(
            import_origins.c.path, import_origins.c.revision, import_origins.c.facts
        ).filter_by(**scope)
    )
    async for path, rev, raw in result:
        facts = json.loads(raw)
        digest = facts.get("commit", baseline)
        present = bool(facts.get("blob") or facts.get("sha256"))
        await edit(con, scope, digest, path, rev, present)


async def redact(con, scope, path):
    # Commits with no remaining paths have no user-visible lookup or log entry.
    await con.execute(sa.delete(edits).filter_by(**scope, path=path))


def _public(row):
    return {
        "hash": row["digest"],
        "subject": row["subject"],
        "date": row["date"],
        "author": row["author"],
        "provenance": json.loads(row["provenance"]),
    }


def _visible(scope, path=None):
    filt = sa.select(edits.c.digest).filter_by(**scope)
    if path:
        # Path/folder names are exact string values, never LIKE expressions.
        if (
            path.startswith("/")
            or "\x00" in path
            or any(p in ("", ".", "..") for p in path.split("/"))
        ):
            raise ValueError("Invalid history path")
        filt = filt.where(
            sa.or_(
                edits.c.path == path,
                edits.c.path.startswith(path + "/", autoescape=True),
            )
        )
    return filt


async def _find(con, scope, digest):
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{7,64}", digest):
        raise ValueError("Invalid revision identifier")
    rows = (
        (
            await con.execute(
                sa.select(commits)
                .filter_by(**scope)
                .where(
                    commits.c.digest.in_(_visible(scope)),
                    commits.c.digest.startswith(digest),
                )
                .limit(2)
            )
        )
        .mappings()
        .all()
    )
    if len(rows) != 1:
        raise LookupError("Revision not found")
    return rows[0]


async def state(con, scope, digest):
    # First-parent commits carry a sparse diff against that parent. A recursive
    # CTE chooses the latest revision on this ancestry separately for each path.
    if digest is None:
        return {}
    chain = (
        sa.select(commits.c.digest, commits.c.parent, sa.literal(0).label("depth"))
        .filter_by(**scope, digest=digest)
        .cte(recursive=True)
    )
    chain = chain.union_all(
        sa.select(commits.c.digest, commits.c.parent, chain.c.depth + 1)
        .select_from(commits.join(chain, commits.c.digest == chain.c.parent))
        .where(*(getattr(commits.c, k) == v for k, v in scope.items()))
    )
    ranked = (
        sa.select(
            edits.c.path,
            edits.c.revision,
            edits.c.present,
            sa.func.row_number()
            .over(partition_by=edits.c.path, order_by=chain.c.depth)
            .label("n"),
        )
        .select_from(edits.join(chain, edits.c.digest == chain.c.digest))
        .where(*(getattr(edits.c, k) == v for k, v in scope.items()))
        .subquery()
    )
    return dict(
        (
            await con.execute(
                sa.select(ranked.c.path, ranked.c.revision).where(
                    ranked.c.n == 1, ranked.c.present.is_(True)
                )
            )
        ).all()
    )


async def _content(con, scope, path, revision):
    if revision is None:
        return None
    value = (
        await con.execute(
            sa.select(versions.c.content).filter_by(
                **scope, path=path, revision=revision
            )
        )
    ).scalar_one_or_none()
    if value is None:
        raise ValueError("Historical revision unavailable")
    return value


async def history(store, context, *, path=None, limit=50):
    scope = await store._scope(context, "memory.read")
    async with store.engine.connect() as con:
        rows = (
            (
                await con.execute(
                    sa.select(commits)
                    .filter_by(**scope)
                    .where(commits.c.digest.in_(_visible(scope, path)))
                    .order_by(commits.c.serial.desc())
                    .limit(limit)
                )
            )
            .mappings()
            .all()
        )
    await store._recheck(context, "memory.read", scope)
    return {"commits": [_public(r) for r in rows], "path": path}


async def detail(store, context, digest):
    scope = await store._scope(context, "memory.read")
    async with store.engine.connect() as con:
        record = await _find(con, scope, digest)
        digest = record["digest"]
        prior = await state(con, scope, record["parent"])
        paths = (
            await con.execute(
                sa.select(edits.c.path, edits.c.revision, edits.c.present)
                .filter_by(**scope, digest=digest)
                .order_by(edits.c.path)
            )
        ).all()
        files = []
        lines = []
        size = 0
        truncated = False
        for path, rev, present in paths:
            status = "M" if present and path in prior else "A" if present else "D"
            files.append({"path": path, "status": status})
            if truncated:
                continue
            old = await _content(con, scope, path, prior.get(path))
            new = await _content(con, scope, path, rev) if present else None
            # Bound diff CPU as well as response bytes for very large rows.
            if sum(len(s) for s in (old or "", new or "")) > 256_000:
                truncated = True
                continue
            for line in difflib.unified_diff(
                (old or "").splitlines(),
                (new or "").splitlines(),
                fromfile="a/" + path if old is not None else "/dev/null",
                tofile="b/" + path if new is not None else "/dev/null",
                lineterm="",
            ):
                size += len(line.encode()) + 1
                if size > 64_000:
                    truncated = True
                    break
                lines.append(line)
    await store._recheck(context, "memory.read", scope)
    return {
        **_public(record),
        "full_hash": digest,
        "files": files,
        "diff": "\n".join(lines),
        "diff_truncated": truncated,
    }


async def restore(store, context, digest, *, authorize, reset=False):
    from .privacy import require_unprotected_execution

    await require_unprotected_execution("Document vault history")
    scope = await store._scope(context, "memory.write")
    async with store.engine.begin() as con:
        head = await lock_head(con, scope)
        target = await _find(con, scope, digest)
        target_id = target["digest"]
        discarded = []
        if reset:
            cursor = head[0]
            while cursor != target_id:
                row = (
                    await con.execute(
                        sa.select(commits.c.source, commits.c.parent).filter_by(
                            **scope, digest=cursor
                        )
                    )
                ).one_or_none()
                if row is None or row.source != "native":
                    return {
                        "error": "Only the native revision tail can be reset; captured source history is retained"
                    }
                discarded.append(cursor)
                cursor = row.parent
        discarded_revisions = set()
        if discarded:
            for removed in discarded:
                discarded_revisions.update(
                    (
                        await con.execute(
                            sa.select(edits.c.path, edits.c.revision).filter_by(
                                **scope, digest=removed
                            )
                        )
                    ).all()
                )
        desired = await state(con, scope, target_id)
        rows = (
            await con.execute(
                sa.select(
                    notes.c.path,
                    notes.c.revision,
                    notes.c.content.is_not(None).label("present"),
                ).filter_by(**scope)
            )
        ).all()
        known = {row.path: row for row in rows}
        if set(desired) - set(known):
            raise ValueError("Target revision lacks a current document identity")
        changed = []
        for path, row in known.items():
            old = (
                await _content(con, scope, path, row.revision) if row.present else None
            )
            new = await _content(con, scope, path, desired.get(path))
            if old == new and not (
                row.present and (path, row.revision) in discarded_revisions
            ):
                continue
            rev = await store._apply(
                con, context, scope, path, new, row.revision, historical=True
            )
            changed.append((path, rev, new is not None))
        if reset:
            for removed in discarded:
                values = (
                    await con.execute(
                        sa.select(edits.c.path, edits.c.revision).filter_by(
                            **scope, digest=removed
                        )
                    )
                ).all()
                for path, rev in values:
                    await con.execute(
                        sa.delete(versions).filter_by(**scope, path=path, revision=rev)
                    )
                await con.execute(sa.delete(edits).filter_by(**scope, digest=removed))
                await con.execute(sa.delete(commits).filter_by(**scope, digest=removed))
            await _advance(con, scope, target_id, head[1])
            result = {"ok": True, "head": target_id, "deleted": len(discarded)}
        else:
            new_head = (
                (
                    await native_commit(
                        con,
                        scope,
                        head,
                        context,
                        changed,
                        "Restored document space",
                        restored=target_id,
                    )
                )
                if changed
                else None
            )
            result = {
                "ok": True,
                "commit": new_head,
                "restored_from": target_id,
                "changed": bool(changed),
            }
        await store._recheck(context, "memory.write", scope)
        await authorize()
    return result
