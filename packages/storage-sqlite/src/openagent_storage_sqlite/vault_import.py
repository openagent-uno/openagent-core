"""Administrative, resumable import of an attested historical corpus."""

import asyncio
from contextlib import closing
from hashlib import sha256
from itertools import islice
import json
import os
from pathlib import Path
import shutil
import sqlite3

import sqlalchemy as sa
from openagent_core.contracts import canonical_json
from openagent_core.memory.operational.privacy_snapshots import (
    guard_snapshot_transaction,
)
from openagent_core.memory.vault.documents import (
    identity_columns,
    import_origins,
    links,
    notes,
    refresh_indexes,
    versions,
)
from openagent_core.memory.vault.parser import link_key, parse_note_text
from openagent_core.runtime import current_run_id
from .migration import _check_transaction_tenant, _digest, _relative
from .vault_git_source import VaultImportMismatch, VaultSource

migration = sa.MetaData()
receipts = sa.Table(
    "oa_vault_import_receipts",
    migration,
    *identity_columns()[:3],
    sa.Column("identity", sa.Text, nullable=False),
    sa.Column("complete", sa.Boolean, nullable=False),
)
inbox = sa.Table(
    "oa_vault_import_inbox",
    migration,
    *identity_columns(),
    sa.Column("revision", sa.BigInteger, primary_key=True),
    sa.Column("content", sa.Text),
    sa.Column("facts", sa.Text, nullable=False),
)


def _binding(src, db, scope):
    name = str(_relative(db))
    if name not in src.manifest["databases"]:
        raise VaultImportMismatch("An attested canonical database is required")
    with closing(
        sqlite3.connect((src.root / name).as_uri() + "?mode=ro&immutable=1", uri=True)
    ) as con:
        con.execute("PRAGMA query_only=ON")
        con.execute("BEGIN")
        guard_snapshot_transaction(con)
        _check_transaction_tenant(con, scope["tenant"])
        if not con.execute(
            "SELECT 1 FROM sessions_v2 WHERE tenant_id=? LIMIT 1", (scope["tenant"],)
        ).fetchone():
            raise VaultImportMismatch(
                "Declared tenant has no canonical session evidence"
            )
    return canonical_json([1, src.receipt, src.relative, name, src.corpus, scope])


def _insert(con, table, values):
    if con.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    return (
        insert(table)
        .values(**values)
        .on_conflict_do_nothing(index_elements=list(table.primary_key.columns))
    )


async def _lock(con, scope, ident):
    row = (
        await con.execute(
            sa.update(receipts)
            .filter_by(**scope, identity=ident)
            .values(identity=ident)
            .returning(receipts.c.complete)
        )
    ).one_or_none()
    if row is None:
        raise VaultImportMismatch("Space is bound to another sealed source or corpus")
    return row.complete


def _add_witness(hashes, path, revision, content, facts):
    digest = hashes.setdefault(path, sha256())
    value = canonical_json([revision, content, facts]).encode()
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


async def _chunks(src):
    changes = iter(src.revisions())
    while chunk := await asyncio.to_thread(lambda: list(islice(changes, 32))):
        yield chunk


async def _stage(con, scope, chunk):
    for path, rev, content, facts in chunk:
        key = {**scope, "path": path, "revision": rev}
        value = canonical_json(facts)
        await con.execute(
            _insert(con, inbox, {**key, "content": content, "facts": value})
        )
        existing = (
            await con.execute(
                sa.select(inbox.c.content, inbox.c.facts).filter_by(**key)
            )
        ).one()
        if tuple(existing) != (content, value):
            raise VaultImportMismatch("Staged revision differs from source evidence")


async def _publish(con, vault, scope, src):
    hashes = {}
    result = await con.stream(
        sa.select(inbox).filter_by(**scope).order_by(inbox.c.path, inbox.c.revision)
    )
    async for row in result.mappings():
        path, rev, content = row["path"], row["revision"], row["content"]
        key = {**scope, "path": path}
        facts = json.loads(row["facts"])
        await con.execute(
            sa.insert(import_origins).values(**key, revision=rev, facts=row["facts"])
        )
        if content is not None:
            # A historical author is not an authenticated application principal.
            author = canonical_json(
                {"source": facts["source"], "author": facts.get("author")}
            )
            await con.execute(
                sa.insert(versions).values(
                    **key,
                    revision=rev,
                    content=content,
                    author=author,
                    session_id="import:" + src.receipt,
                    run_id=None,
                )
            )
        if facts["source"] == "working_tree":
            parsed = parse_note_text(path, content) if content is not None else None
            note_id = (
                await con.execute(
                    sa.insert(notes)
                    .values(
                        **key,
                        revision=rev,
                        content=content,
                        title=parsed.title if parsed else None,
                    )
                    .returning(notes.c.id)
                )
            ).scalar_one()
            await refresh_indexes(con, key, note_id, parsed, content)
        _add_witness(hashes, path, rev, content, facts)
    from openagent_core.memory.vault.document_history import seed_import

    await seed_import(con, scope, src)
    return {k: v.digest() for k, v in hashes.items()}


async def verify_import(vault, context, src, database, *, allow_new_writes=False):
    """Re-read source revisions and public head data before enabling writers."""
    if current_run_id():
        raise PermissionError("An offline host is required")
    scope = await vault._scope(context, "memory.import")
    ident = await asyncio.to_thread(_binding, src, database, scope)
    await asyncio.to_thread(src.verify_files)
    total = heads = texts = 0
    async with vault.engine.connect() as con:
        complete = (
            await con.execute(
                sa.select(receipts.c.complete).filter_by(**scope, identity=ident)
            )
        ).scalar_one_or_none()
        if complete is not True:
            raise VaultImportMismatch("Missing completed scope receipt")
        async for chunk in _chunks(src):
            for path, rev, content, facts in chunk:
                key = {**scope, "path": path}
                evidence = (
                    await con.execute(
                        sa.select(import_origins.c.facts).filter_by(**key, revision=rev)
                    )
                ).scalar_one_or_none()
                if evidence != canonical_json(facts):
                    raise VaultImportMismatch("Revision provenance differs from source")
                actual = (
                    await con.execute(
                        sa.select(
                            versions.c.content,
                            versions.c.author,
                            versions.c.session_id,
                            versions.c.run_id,
                        ).filter_by(**key, revision=rev)
                    )
                ).one_or_none()
                expected = (
                    None
                    if content is None
                    else (
                        content,
                        canonical_json(
                            {"source": facts["source"], "author": facts.get("author")}
                        ),
                        "import:" + src.receipt,
                        None,
                    )
                )
                if (tuple(actual) if actual is not None else None) != expected:
                    raise VaultImportMismatch("Revision differs from sealed source")
                texts += content is not None
                if facts["source"] == "working_tree":
                    head = (
                        await con.execute(
                            sa.select(
                                notes.c.content,
                                notes.c.revision,
                                notes.c.title,
                                notes.c.id,
                            ).filter_by(**key)
                        )
                    ).one_or_none()
                    newer = (
                        allow_new_writes and head is not None and head.revision > rev
                    )
                    if not newer and (
                        head is None or head.content != content or head.revision != rev
                    ):
                        raise VaultImportMismatch(
                            "Current note differs from working tree"
                        )
                    if not newer:
                        parsed = (
                            parse_note_text(path, content)
                            if content is not None
                            else None
                        )
                        targets = (
                            {
                                link_key(link)
                                for link in parsed.outlinks
                                if len(link_key(link).encode()) <= 700
                            }
                            if parsed
                            else set()
                        )
                        indexed = (
                            (
                                await con.execute(
                                    sa.select(links.c.target).filter_by(**key)
                                )
                            )
                            .scalars()
                            .all()
                        )
                        if set(indexed) != targets or head.title != (
                            parsed.title if parsed else None
                        ):
                            raise VaultImportMismatch(
                                "Document index differs from the working tree"
                            )
                        if con.dialect.name == "sqlite":
                            fts = (
                                await con.execute(
                                    sa.text(
                                        "SELECT content FROM oa_vault_search WHERE rowid=:id"
                                    ),
                                    {"id": head.id},
                                )
                            ).scalar_one_or_none()
                            if fts != content:
                                raise VaultImportMismatch(
                                    "Full-text index differs from the working tree"
                                )
                    heads += 1
                total += 1
        for table, count in (
            (notes, heads),
            (versions, texts),
            (import_origins, total),
            (inbox, 0),
        ):
            actual = (
                await con.execute(
                    sa.select(sa.func.count()).select_from(table).filter_by(**scope)
                )
            ).scalar_one()
            if actual != count and not (
                allow_new_writes and table in (notes, versions) and actual > count
            ):
                raise VaultImportMismatch("Document scope count differs")
        await vault._recheck(context, "memory.import", scope)
    return {
        "scope": "verified_document_corpus",
        "selected_paths": heads,
        "source_revisions": total,
        "content_revisions": texts,
        "source_manifest_sha256": src.receipt,
        "corpus_sha256": src.corpus,
    }


async def import_vault(vault, context, src: VaultSource, database: str):
    """Stage each chunk durably. Publish the entire previously empty space in one transaction."""
    if current_run_id():
        raise PermissionError("An offline host is required")
    scope = await vault._scope(context, "memory.import")
    ident = await asyncio.to_thread(_binding, src, database, scope)
    async with vault.engine.begin() as con:
        await con.run_sync(migration.create_all)
        await con.execute(
            _insert(con, receipts, {**scope, "identity": ident, "complete": False})
        )
        done = await _lock(con, scope, ident)
        if (
            not done
            and (
                await con.execute(sa.select(notes.c.id).filter_by(**scope).limit(1))
            ).first()
        ):
            raise VaultImportMismatch("A new document space is required")
    if not done:
        hashes = {}
        async for chunk in _chunks(src):
            async with vault.engine.begin() as con:
                if await _lock(con, scope, ident):
                    break
                await _stage(con, scope, chunk)
                await vault._recheck(context, "memory.import", scope)
            for path, rev, content, facts in chunk:
                _add_witness(hashes, path, rev, content, facts)
        async with vault.engine.begin() as con:
            if not await _lock(con, scope, ident):
                if (
                    await con.execute(sa.select(notes.c.id).filter_by(**scope).limit(1))
                ).first():
                    raise VaultImportMismatch(
                        "Document space was used before publication"
                    )
                if await _publish(con, vault, scope, src) != {
                    k: v.digest() for k, v in hashes.items()
                }:
                    raise VaultImportMismatch(
                        "Import received unexpected staged revisions"
                    )
                await con.execute(sa.delete(inbox).filter_by(**scope))
                await con.execute(
                    sa.update(receipts).filter_by(**scope).values(complete=True)
                )
                await vault._recheck(context, "memory.import", scope)
    return await verify_import(vault, context, src, database)


def _restore_private_tree(src, state):
    for name, entry in src.manifest["files"].items():
        source = src.root / name
        if not source.is_relative_to(src.vault):
            continue
        target = state / name
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copyfile(source, target)
        if _digest(target) != entry["sha256"]:
            raise VaultImportMismatch("Restored archive differs from the sealed source")
        os.chmod(target, entry["mode"] & 0o777)


async def restore_vault(
    vault, context, src, database, destination, *, new_paths=(), quiesce
):
    """Restore a new private filesystem candidate and export all later document revisions.

    The host fences SQL writers throughout the rollback drill. The sealed source
    is never changed. A missing completion receipt identifies an interrupted drill.
    """
    from openagent_core.memory.vault.documents import note_path

    if current_run_id() or not callable(quiesce) or isinstance(new_paths, str):
        raise PermissionError("Offline host fence and explicit restore corpus required")
    scope = await vault._scope(context, "memory.import")
    ident = await asyncio.to_thread(_binding, src, database, scope)
    additions = {note_path(path) for path in new_paths}
    if additions.intersection(src.paths) or any(
        f"{src.relative}/{p}" in src.manifest["files"] for p in additions
    ):
        raise VaultImportMismatch("A new path collides with a sealed file")
    destination = Path(destination).resolve()
    if destination.is_relative_to(src.root):
        raise VaultImportMismatch(
            "Restore requires a location outside the sealed source"
        )
    projected = changes = 0
    async with quiesce() as fence, vault.engine.connect() as con:
        if not isinstance(fence, str) or not fence.strip():
            raise VaultImportMismatch("A host writer fence is required")
        done = (
            await con.execute(
                sa.select(receipts.c.complete).filter_by(**scope, identity=ident)
            )
        ).scalar_one_or_none()
        if done is not True:
            raise VaultImportMismatch("Restore source and completed import differ")
        rows = (
            await con.execute(
                sa.select(notes.c.path, notes.c.revision)
                .filter_by(**scope)
                .order_by(notes.c.path)
            )
        ).all()
        if {r.path for r in rows} != src.paths | additions:
            raise VaultImportMismatch(
                "Every document requires an attested restore path"
            )
        # A missing historical witness after an explicit forget is an erasure.
        # Re-serving a Git copy with its old content would undo that erasure.
        for row in rows:
            if row.path in src.paths:
                witness = (
                    await con.execute(
                        sa.select(import_origins.c.facts)
                        .filter_by(**scope, path=row.path)
                        .order_by(import_origins.c.revision.desc())
                        .limit(1)
                    )
                ).scalar_one_or_none()
                if witness is None or json.loads(witness)["source"] != "working_tree":
                    raise VaultImportMismatch(
                        "Restore needs privacy-qualified historical erasure"
                    )
        await verify_import(vault, context, src, database, allow_new_writes=True)
        destination.mkdir(mode=0o700, parents=False, exist_ok=False)
        state = destination / "state"
        await asyncio.to_thread(_restore_private_tree, src, state)
        log = destination / "post-import-revisions.jsonl"
        with log.open("xb") as stream:
            os.chmod(log, 0o600)
            for row in rows:
                path = row.path
                base = (
                    await con.execute(
                        sa.select(sa.func.max(import_origins.c.revision)).filter_by(
                            **scope, path=path
                        )
                    )
                ).scalar_one() or 0
                content = (
                    await con.execute(
                        sa.select(notes.c.content).filter_by(**scope, path=path)
                    )
                ).scalar_one()
                target = state / src.relative / path
                from openagent_core.memory.vault.document_history import commits, edits

                written = (
                    sa.select(
                        versions.c.revision,
                        versions.c.content,
                        versions.c.author,
                        versions.c.session_id,
                        versions.c.run_id,
                    )
                    .filter_by(**scope, path=path)
                    .where(versions.c.revision > base)
                )
                join = sa.and_(
                    commits.c.digest == edits.c.digest,
                    *(getattr(commits.c, k) == getattr(edits.c, k) for k in scope),
                )
                # JSON trailers are deliberately stored as portable text; retrieve them
                # directly to avoid backend-dependent JSON casts on the native journal.
                tombstones = (
                    await con.execute(
                        sa.select(
                            edits.c.revision, commits.c.author, commits.c.provenance
                        )
                        .select_from(edits.join(commits, join))
                        .where(
                            *(getattr(edits.c, k) == v for k, v in scope.items()),
                            edits.c.path == path,
                            edits.c.revision > base,
                            edits.c.present.is_(False),
                            commits.c.source == "native",
                        )
                    )
                ).all()
                markers = [
                    {
                        "revision": r,
                        "content": None,
                        "author": a,
                        "session_id": json.loads(o).get("session"),
                        "run_id": json.loads(o).get("run"),
                    }
                    for r, a, o in tombstones
                ]
                from heapq import merge

                records = merge(
                    (
                        await con.execute(written.order_by(versions.c.revision))
                    ).mappings(),
                    sorted(markers, key=lambda x: x["revision"]),
                    key=lambda x: x["revision"],
                )
                last = base
                latest = None
                for record in records:
                    if record["revision"] != last + 1:
                        raise VaultImportMismatch(
                            "Post-import revision history has a gap"
                        )
                    latest = record["content"]
                    last = record["revision"]
                    stream.write(
                        (canonical_json({"path": path, **record}) + "\n").encode()
                    )
                    changes += 1
                if last != row.revision or (last > base and latest != content):
                    raise VaultImportMismatch(
                        "Post-import head lacks its revision ledger"
                    )
                if content is None:
                    if last > base and target.is_file():
                        target.unlink()  # only the newly restored, verified candidate
                    if target.exists():
                        raise VaultImportMismatch(
                            "Restored archive conflicts with a forgotten document"
                        )
                    continue
                value = content.encode()
                if last > base:
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    target.write_bytes(value)
                if target.read_bytes() != value:
                    raise VaultImportMismatch(
                        "Restored working tree differs from the SQL document"
                    )
                projected += 1
        await vault._recheck(context, "memory.import", scope)
        receipt = {
            "scope": "private_vault_restore",
            "source_manifest_sha256": src.receipt,
            "corpus_sha256": src.corpus,
            "working_documents": projected,
            "later_revisions": changes,
            "revision_ledger_sha256": _digest(log),
        }
        (destination / "complete.json").write_text(canonical_json(receipt) + "\n")
    return receipt
