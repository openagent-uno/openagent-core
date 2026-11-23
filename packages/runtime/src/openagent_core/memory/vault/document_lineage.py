"""Erase contaminated revision intervals and keep independent note incarnations."""
import sqlalchemy as sa

from .document_history import edits, native_commit
from .documents import dependencies, import_origins, moves, notes, read_receipts, refresh_indexes, versions

_LAST = (1 << 63) - 1


async def _affected_intervals(con, scope, root):
    # Edits after an observation can retain the observed bytes without reading
    # that source again. A completed move ends that incarnation, so a later
    # independently recreated file at the old name remains usable.
    edge = dependencies
    end_of_lifetime = (sa.select(sa.func.min(moves.c.last_revision) + 1)
        .filter_by(**scope).where(moves.c.path == edge.c.path, moves.c.last_revision >= edge.c.revision)
        .correlate(edge).scalar_subquery())
    dependency = sa.select(edge.c.source_path.label("source"), edge.c.source_revision.label("observed"),
        edge.c.path.label("target"), edge.c.revision.label("first"),
        sa.func.coalesce(end_of_lifetime, _LAST).label("last")).filter_by(**scope)
    # A rename keeps the identity of the moved document in both directions.
    # Its old history is part of an explicit forget at the new name.
    predecessor = sa.select(moves.c.destination, moves.c.destination_revision,
                           moves.c.path, moves.c.first_revision, moves.c.last_revision + 1).filter_by(**scope)
    edges = dependency.union_all(predecessor).subquery()
    reached = sa.select(sa.cast(sa.literal(root), sa.String(700)).label("path"),
                        sa.cast(0, sa.BigInteger).label("first"),
                        sa.cast(_LAST, sa.BigInteger).label("last")).cte(recursive=True)
    reached = reached.union(sa.select(edges.c.target, edges.c.first, edges.c.last).select_from(
        reached.join(edges, sa.and_(edges.c.source == reached.c.path,
                                    edges.c.observed.between(reached.c.first, reached.c.last)))))
    intervals = {}
    for path, first, last in (await con.execute(sa.select(reached).order_by(reached.c.path, reached.c.first))):
        ranges = intervals.setdefault(path, [])
        if ranges and first <= ranges[-1][1] + 1:
            ranges[-1] = (ranges[-1][0], max(ranges[-1][1], last))
        else:
            ranges.append((first, last))
    return intervals


async def forget_derivations(con, vault, scope, head, context, root):
    intervals = await _affected_intervals(con, scope, root)
    recovered = []
    for path, spans in intervals.items():
        key = {**scope, "path": path}
        current = await con.scalar(sa.select(notes.c.revision).filter_by(**key))
        previous = None
        for first, last in spans:
            if path != root and current is not None and first <= current <= last:
                previous = (await con.execute(sa.select(edits.c.present, edits.c.revision)
                    .filter_by(**key).where(edits.c.revision < first)
                    .order_by(edits.c.revision.desc()).limit(1))).first()
                # Keep an existing historical tombstone at a moved-away name.
                live = await con.scalar(sa.select(notes.c.content.is_not(None)).filter_by(**key))
                if not live and last != _LAST:
                    previous = None
                if previous and previous.present:
                    content = await con.scalar(sa.select(versions.c.content).filter_by(**key, revision=previous.revision))
                    if content is None:
                        raise RuntimeError("Independent document revision is unavailable")
                    rev = await vault._apply(con, context, scope, path, content, current, historical=True)
                    recovered.append((path, rev, True))
                else:
                    row_id = await con.scalar(notes.update().filter_by(**key, revision=current)
                        .values(revision=current + 1, title=None, content=None).returning(notes.c.id))
                    await refresh_indexes(con, key, row_id, None, None)
            for table in (read_receipts, import_origins, versions, edits, dependencies):
                upper = min(last, current) if current is not None else last
                await con.execute(table.delete().filter_by(**key).where(table.c.revision.between(first, upper)))
            await con.execute(dependencies.delete().filter_by(**scope, source_path=path)
                              .where(dependencies.c.source_revision.between(first, last)))
            await con.execute(moves.delete().filter_by(**scope, destination=path)
                              .where(moves.c.destination_revision.between(first, last)))
            await con.execute(moves.delete().filter_by(**key).where(moves.c.last_revision.between(first, last)))
    if recovered:
        return await native_commit(con, scope, head, context, recovered, "Restore unaffected note revisions")
