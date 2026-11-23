"""Remove only the affected revision tails when a note's source is forgotten."""
import sqlalchemy as sa

from .document_history import edits, native_commit
from .documents import dependencies, import_origins, notes, read_receipts, refresh_indexes, versions


async def forget_derivations(con, vault, scope, head, context, root):
    # Compute the transitive closure before editing any edges. UNION also
    # terminates when two notes have read successive revisions of one another.
    affected = sa.select(sa.literal(root).label("path"), sa.cast(0, sa.BigInteger).label("cut")).cte(
        "affected", recursive=True)
    edge = dependencies
    affected = affected.union(sa.select(edge.c.path, edge.c.revision).where(
        *(edge.c[k] == v for k,v in scope.items()),
        edge.c.source_path == affected.c.path, edge.c.source_revision >= affected.c.cut))
    cutoffs = (await con.execute(sa.select(affected.c.path, sa.func.min(affected.c.cut))
                                .group_by(affected.c.path))).all()
    recovered = []
    for path, cutoff in cutoffs:
        key = {**scope, "path": path}
        await con.execute(read_receipts.delete().filter_by(**key).where(read_receipts.c.revision >= cutoff))
        if path != root:
            # Later edits can have inherited text without reading a second source.
            # Preserve all independent earlier revisions and restore their latest state.
            previous = (await con.execute(sa.select(edits.c.present, edits.c.revision)
                .filter_by(**key).where(edits.c.revision < cutoff)
                .order_by(edits.c.revision.desc()).limit(1))).first()
            content = None
            if previous and previous.present:
                content = await con.scalar(sa.select(versions.c.content).filter_by(**key, revision=previous.revision))
                if content is None:
                    raise RuntimeError("Independent document revision is unavailable")
            current = await con.scalar(sa.select(notes.c.revision).filter_by(**key))
            if current is not None:
                if content is None:
                    # A previously deleted derived note can still have private history.
                    row_id = await con.scalar(notes.update().filter_by(**key, revision=current)
                        .values(revision=current + 1, title=None, content=None).returning(notes.c.id))
                    await refresh_indexes(con, key, row_id, None, None)
                else:
                    new_revision = await vault._apply(con,context,scope,path,content,current,historical=True)
                    recovered.append((path,new_revision,True))
                for table in (versions, import_origins, edits):
                    await con.execute(table.delete().filter_by(**key).where(
                        table.c.revision >= cutoff, table.c.revision <= current))
        await con.execute(edge.delete().filter_by(**key).where(edge.c.revision >= cutoff))
        await con.execute(edge.delete().filter_by(**scope, source_path=path).where(edge.c.source_revision >= cutoff))
    if recovered:
        return await native_commit(con,scope,head,context,recovered,"Restore unaffected note revisions")
