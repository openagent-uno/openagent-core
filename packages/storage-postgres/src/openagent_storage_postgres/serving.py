"""Fenced changes of serving build over one authoritative cloud installation."""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from uuid import UUID

import sqlalchemy as sa
from openagent_core.archived_artifacts import SqlArtifactArchive
from openagent_core.contracts import ServingUnavailable, canonical_json
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_core.object_artifacts import S3Chunks, SqlObjectArtifacts
from psycopg import Error, sql
from psycopg.errors import LockNotAvailable
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from openagent_storage_postgres import PostgresRuntimeStore


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError("Expected a pinned SHA-256")
    return value


class PostgresServingGeneration:
    """Open a separate bounded connection pool for the optional deployment fence."""

    def __init__(self, store, *, deployment, generation, binding_sha256, pool_size=4):
        if not isinstance(store, PostgresRuntimeStore):
            raise TypeError("The actual runtime PostgreSQL store is required")
        if not isinstance(deployment, str) or not deployment.strip() or len(deployment) > 200:
            raise ValueError("Expected an immutable deployment identity")
        if type(generation) is not int or generation < 1:
            raise ValueError("Invalid serving generation")
        self.store, self.deployment, self.generation = store, deployment, generation
        self.binding_sha256 = _sha(binding_sha256)
        if type(pool_size) is not int or pool_size < 2:
            raise ValueError("Reserve separate admission and operator connections")
        self._lock = asyncio.Lock()
        self._holders = 0
        self._guard = self._guard_db = None
        self.pool = AsyncConnectionPool(
            store._dsn, min_size=0, max_size=pool_size, open=False, kwargs={"row_factory": dict_row}
        )

    async def open(self):
        await self.pool.open()

    async def close(self):
        await self.pool.close()

    @asynccontextmanager
    async def _db(self):
        async with self.pool.connection() as db, db.transaction():
            await db.execute(
                sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                    sql.Identifier(self.store.schema)
                )
            )
            yield db

    async def install(self):
        """Explicit operator bootstrap. A restart cannot reset an existing generation."""
        if self.generation != 1:
            raise ValueError("Only the first generation can bootstrap")
        async with self._db() as db:
            await db.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("openagent:" + self.store.schema,),
            )
            await db.execute("""CREATE TABLE IF NOT EXISTS oa_serving_route (
                agent text PRIMARY KEY, deployment text NOT NULL, generation bigint NOT NULL,
                binding_sha256 text NOT NULL)""")
            await db.execute("""CREATE TABLE IF NOT EXISTS oa_serving_changes (
                agent text NOT NULL, change_id uuid NOT NULL, receipt jsonb NOT NULL,
                PRIMARY KEY (agent,change_id))""")
            await db.execute(
                "INSERT INTO oa_serving_route VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (self.store.agent_id, self.deployment, self.generation, self.binding_sha256),
            )
            self._check(await self._row(db, sql.SQL("FOR UPDATE")))

    async def _row(self, db, lock):
        query = sql.SQL(
            "SELECT deployment,generation,binding_sha256 FROM oa_serving_route WHERE agent=%s {}"
        ).format(lock)
        return await (await db.execute(query, (self.store.agent_id,))).fetchone()

    def _check(self, row):
        if not row or (row["deployment"], row["generation"], row["binding_sha256"]) != (
            self.deployment,
            self.generation,
            self.binding_sha256,
        ):
            raise ServingUnavailable("This deployment generation cannot serve")

    async def receipt(self, change_id: UUID):
        if not isinstance(change_id, UUID):
            raise TypeError("A change UUID is required")
        async with self._db() as db:
            row = await (
                await db.execute(
                    "SELECT receipt FROM oa_serving_changes WHERE agent=%s AND change_id=%s",
                    (self.store.agent_id, change_id),
                )
            ).fetchone()
            return row["receipt"] if row else None

    @asynccontextmanager
    async def _shared_guard(self):
        async with self._db() as db:
            self._check(await self._row(db, sql.SQL("FOR SHARE")))
            yield db

    @asynccontextmanager
    async def serving(self):
        """Overlapping requests share one held database lock per replica."""
        async with self._lock:
            if self._holders:
                if self._guard_db.closed or self._guard_db.broken:
                    raise ServingUnavailable("The serving fence connection was lost")
            else:
                guard = self._shared_guard()
                try:
                    self._guard_db = await guard.__aenter__()
                except (Error, TimeoutError) as exc:
                    raise ServingUnavailable(
                        "The serving generation could not be verified"
                    ) from exc
                self._guard = guard
            self._holders += 1
        try:
            yield
        finally:
            async with self._lock:
                self._holders -= 1
                if not self._holders:
                    guard, self._guard, self._guard_db = self._guard, None, None
                    await guard.__aexit__(None, None, None)


@dataclass(frozen=True)
class ContractEvidence:
    """Independent current reads from each deployed contract. Keep payloads private."""

    allowed_reads_sha256: str
    denied_reads_sha256: str
    effect_receipts_sha256: str

    def __post_init__(self):
        for digest in asdict(self).values():
            _sha(digest)


@dataclass(frozen=True)
class CloudServingTarget:
    route: PostgresServingGeneration
    artifacts: SqlObjectArtifacts
    documents: SqlDocumentVault
    probe: Callable[[], Awaitable[ContractEvidence]]


async def _same_sql_engine(source, target, relations):
    """A fresh conflicting lock proves a live database, including through DSN aliases."""
    lock = secrets.randbits(63)
    async with source.begin() as left, target.begin() as right:
        await left.execute(sa.text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
        separate = await right.scalar(
            sa.text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": lock}
        )
        if separate:
            raise ValueError("The target uses a different authoritative database")
        for name, privileges in relations:
            stmt = sa.text("""
                SELECT to_regclass(:name)::oid AS oid,
                    (SELECT bool_and(has_table_privilege(to_regclass(:name), privilege))
                     FROM unnest(string_to_array(:privileges, ',')) AS privilege) AS permitted,
                    NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off' AS writable
            """)
            rows = []
            for con in (left, right):
                record = (
                    (await con.execute(stmt, {"name": name, "privileges": privileges}))
                    .mappings()
                    .one()
                )
                if record["oid"] is None or not record["permitted"]:
                    raise ValueError(
                        "The target lacks the authoritative relation or its privileges"
                    )
                if privileges != "SELECT" and not record["writable"]:
                    raise ValueError("The authoritative writer is read-only")
                rows.append(record["oid"])
            if rows[0] != rows[1]:
                raise ValueError("The target uses a different authoritative relation")


async def _verify_bindings(first, second, active_db):
    a, b = first.route, second.route
    lock = secrets.randbits(63)
    await active_db.execute("SELECT pg_advisory_xact_lock(%s)", (lock,))
    async with b._db() as other:
        row = await (
            await other.execute("SELECT pg_try_advisory_xact_lock(%s) AS separate", (lock,))
        ).fetchone()
        access = await (
            await other.execute("""
            SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off'
                AND bool_and(coalesce(has_table_privilege(to_regclass(rel), privilege),false)) AS writable
            FROM (VALUES ('runs','INSERT'),('runs','UPDATE'),('events','INSERT'),('sessions','INSERT'),
                ('sessions','UPDATE'),('messages','INSERT'),('tool_calls','INSERT'),('tool_calls','UPDATE'),
                ('operations','INSERT'),('operations','UPDATE'),('workers','INSERT'),('workers','UPDATE'))
                AS needed(rel,privilege)
        """)
        ).fetchone()
        if not access["writable"]:
            raise ValueError("The standby lacks runtime writer privileges")
        if (
            row["separate"]
            or a.store.schema != b.store.schema
            or a.store.agent_id != b.store.agent_id
        ):
            raise ValueError("The target uses a different runtime ledger")
    left, right = first.artifacts, second.artifacts
    if not all(
        isinstance(repo, SqlObjectArtifacts) and isinstance(repo.blobs, S3Chunks)
        for repo in (left, right)
    ):
        raise TypeError("Both targets require the actual SQL and S3 repositories")
    await _same_sql_engine(left.engine, right.engine, (("oa_session_artifacts", "SELECT,INSERT"),))
    if not all(isinstance(target.documents, SqlDocumentVault) for target in (first, second)):
        raise TypeError("Both targets require the actual document vault")
    await _same_sql_engine(
        first.documents.engine,
        second.documents.engine,
        (
            ("oa_vault_notes", "SELECT,INSERT,UPDATE"),
            ("oa_vault_revisions", "SELECT,INSERT,DELETE"),
            ("oa_vault_read_receipts", "SELECT,INSERT,DELETE"),
        ),
    )
    if (left.archive is None) != (right.archive is None):
        raise ValueError("The historical artifact binding differs")
    stores = [(left.blobs, right.blobs)]
    if left.archive is not None:
        archives = left.archive, right.archive
        if (
            not all(isinstance(item, SqlArtifactArchive) for item in archives)
            or archives[0].schema != archives[1].schema
        ):
            raise ValueError("The historical archive schema differs")
        await _same_sql_engine(
            archives[0].engine,
            archives[1].engine,
            ((archives[0].schema + ".archive_objects", "SELECT"),),
        )
        if not all(item.blobs is repo.blobs for item, repo in zip(archives, (left, right))):
            stores.append((archives[0].blobs, archives[1].blobs))
    witnesses = []
    for primary, standby in stores:
        if not all(isinstance(store, S3Chunks) for store in (primary, standby)):
            raise TypeError("Both targets require the private S3 store")
        if (primary.bucket, primary.prefix) != (standby.bucket, standby.prefix):
            raise ValueError("The private object namespace differs")
        for writer, reader in ((primary, standby), (standby, primary)):
            nonce = secrets.token_bytes(32)
            digest = hashlib.sha256(nonce).hexdigest()
            key = "serving-witness/" + digest
            await writer.put_immutable(key, nonce)
            if await reader.get(key, len(nonce)) != nonce:
                raise ValueError("The target cannot read the authoritative private object store")
            witnesses.append(digest)
    return witnesses


async def switch_serving_generation(
    source: CloudServingTarget,
    target: CloudServingTarget,
    *,
    change_id: UUID,
    writer_fence,
    timeout_seconds=60,
):
    """Change a single route atomically after the host drains every external writer.

    Probe callbacks run over private read-only channels while public admission is
    fenced. The source stays selected if any check fails. Repeat the same UUID
    to retrieve its receipt after an ambiguous network response.
    """
    a, b = source.route, target.route
    if (
        not isinstance(change_id, UUID)
        or a.store.agent_id != b.store.agent_id
        or a.store.schema != b.store.schema
        or a.binding_sha256 != b.binding_sha256
        or a.deployment == b.deployment
        or b.generation != a.generation + 1
    ):
        raise ValueError("The standby must preserve the binding and advance exactly one generation")
    if type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 3600:
        raise ValueError("Choose a bounded positive operator timeout")
    async with asyncio.timeout(timeout_seconds), writer_fence(a, b) as attestation:
        return await _switch_fenced(source, target, change_id, _sha(attestation))


async def _switch_fenced(source, target, change_id, fence_sha256):
    a, b = source.route, target.route
    async with a._db() as db:
        try:
            row = await a._row(db, sql.SQL("FOR UPDATE NOWAIT"))
        except LockNotAvailable as exc:
            raise ServingUnavailable(
                "Drain in-flight requests and writers before switching"
            ) from exc
        previous = await (
            await db.execute(
                "SELECT receipt FROM oa_serving_changes WHERE agent=%s AND change_id=%s",
                (a.store.agent_id, change_id),
            )
        ).fetchone()
        if previous:
            b._check(row)
            receipt = previous["receipt"]
            if (receipt["source"], receipt["destination"], receipt["generation"]) != (
                a.deployment,
                b.deployment,
                b.generation,
            ):
                raise ServingUnavailable("Change UUID already used for another selection")
            return receipt
        a._check(row)
        pending = await (
            await db.execute(
                """
            SELECT EXISTS(SELECT 1 FROM runs WHERE agent=%s AND status IN ('queued','running'))
                OR EXISTS(SELECT 1 FROM tool_calls WHERE agent=%s AND NOT finished)
                OR EXISTS(SELECT 1 FROM operations WHERE agent=%s AND state='active')
                OR EXISTS(SELECT 1 FROM provider_attempts WHERE agent=%s) AS pending
            """,
                (a.store.agent_id,) * 4,
            )
        ).fetchone()
        if pending["pending"]:
            raise ServingUnavailable("Drain or reconcile pending agent work before switching")
        witnesses = await _verify_bindings(source, target, db)
        evidence, observed = await source.probe(), await target.probe()
        if (
            not isinstance(evidence, ContractEvidence)
            or not isinstance(observed, ContractEvidence)
            or observed != evidence
        ):
            raise ValueError(
                "The standby disagrees on allowed reads, revocations or effect receipts"
            )
        fingerprint = hashlib.sha256(canonical_json(asdict(evidence)).encode()).hexdigest()
        receipt = {
            "format": 1,
            "change_id": str(change_id),
            "source": a.deployment,
            "destination": b.deployment,
            "generation": b.generation,
            "binding_sha256": b.binding_sha256,
            "evidence_sha256": fingerprint,
            "fence_sha256": fence_sha256,
            "object_witnesses": witnesses,
        }
        await db.execute(
            "UPDATE oa_serving_route SET generation=%s,deployment=%s WHERE agent=%s",
            (b.generation, b.deployment, a.store.agent_id),
        )
        await db.execute(
            "INSERT INTO oa_serving_changes VALUES(%s,%s,%s)",
            (a.store.agent_id, change_id, Jsonb(receipt)),
        )
        return receipt
