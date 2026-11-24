"""Transactional reservations shared by the workers of one operator-managed computer pool."""

from dataclasses import asdict, dataclass

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .capabilities import CapabilityUnavailable
from .contracts import canonical_json

metadata = sa.MetaData()
pools = sa.Table(
    "oa_computer_capacity_pool",
    metadata,
    sa.Column("id", sa.String(200), primary_key=True),
    sa.Column("limits", sa.Text, nullable=False),
)
reservations = sa.Table(
    "oa_computer_capacity_reservation",
    metadata,
    sa.Column("allocation", sa.String(64), primary_key=True),
    sa.Column("pool", sa.ForeignKey(pools.c.id), nullable=False),
    sa.Column("owner", sa.Text, nullable=False),
    sa.Column("cpus", sa.BigInteger, nullable=False),
    sa.Column("memory_bytes", sa.BigInteger, nullable=False),
    sa.Column("disk_bytes", sa.BigInteger, nullable=False),
    sa.Column("running", sa.Boolean, nullable=False),
)
_METRICS = ("machines", "disk_bytes", "running", "cpus", "memory_bytes")
counters = sa.Table(
    "oa_computer_capacity_usage",
    metadata,
    sa.Column("pool", sa.ForeignKey(pools.c.id), primary_key=True),
    sa.Column("owner", sa.Text, primary_key=True),
    *(sa.Column(metric, sa.BigInteger, nullable=False) for metric in _METRICS),
    sa.CheckConstraint(" AND ".join(f"{metric} >= 0" for metric in _METRICS)),
)


def _positive(value):
    if type(value) is not int or not 0 < value <= 2**63 - 1:
        raise ValueError("Expected a positive finite computer resource limit")


@dataclass(frozen=True)
class ComputerResources:
    cpus: int
    memory_bytes: int
    disk_bytes: int

    def __post_init__(self):
        for value in asdict(self).values():
            _positive(value)


@dataclass(frozen=True)
class ComputerPoolLimits:
    machines: int
    machines_per_owner: int
    running: int
    running_per_owner: int
    cpus: int
    memory_bytes: int
    disk_bytes: int

    def __post_init__(self):
        for value in asdict(self).values():
            _positive(value)


async def _initialize_computer_schema(con, schema, anchor):
    """Serialize first-use DDL across controllers for both supported databases."""
    if con.dialect.name == "postgresql":
        await con.execute(sa.text("SELECT pg_advisory_xact_lock(689358472606998101)"))
    else:
        in_transaction = await con.run_sync(lambda sync: sync.connection.driver_connection.in_transaction)
        if not in_transaction:
            await con.exec_driver_sql("BEGIN IMMEDIATE")
        else:
            # Hosts can configure the driver to BEGIN themselves. Obtain its
            # writer lock before SQLAlchemy inspects any missing table names.
            await con.execute(sa.schema.CreateTable(anchor, if_not_exists=True))
            primary = next(iter(anchor.primary_key))
            await con.execute(anchor.update().where(sa.false()).values({primary: primary}))
    await con.run_sync(schema.create_all)


class SqlComputerCapacity:
    """One host pool. Reserve before effects; release compute only after physical stop."""

    def __init__(self, engine, pool: str, limits: ComputerPoolLimits):
        if engine.dialect.name not in {"sqlite", "postgresql"}:
            raise ValueError("Use a transactional SQLite or PostgreSQL capacity store")
        if not isinstance(pool, str) or not pool.strip() or len(pool.encode()) > 200:
            raise ValueError("Invalid operator pool identity")
        if not isinstance(limits, ComputerPoolLimits):
            raise TypeError("Expected operator-owned pool limits")
        self.engine, self.pool, self.limits = engine, pool, limits
        self._policy = canonical_json(asdict(limits))

    async def initialize(self):
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        async with self.engine.begin() as con:
            await _initialize_computer_schema(con, metadata, pools)
            await con.execute(
                insert(pools)
                .values(id=self.pool, limits=self._policy)
                .on_conflict_do_nothing(index_elements=[pools.c.id])
            )
            await self._lock(con)

    async def _lock(self, con):
        # A no-op UPDATE obtains the PostgreSQL row lock or SQLite writer lock
        # before counting allocations, including the first machine for an owner.
        row = await con.execute(
            pools.update()
            .where(pools.c.id == self.pool, pools.c.limits == self._policy)
            .values(limits=self._policy)
        )
        if row.rowcount != 1:
            raise CapabilityUnavailable("The admitted pool policy is unavailable or has changed")

    async def _usage(self, con, owner=""):
        result = await con.execute(
            sa.select(*(counters.c[name] for name in _METRICS)).filter_by(pool=self.pool, owner=owner)
        )
        return result.one_or_none() or (0, 0, 0, 0, 0)

    async def _adjust(self, con, owner, *, machines=0, disk_bytes=0, running=0, cpus=0, memory_bytes=0):
        # The pool lock serializes both counter keys. Update the two keys in
        # the reservation transaction so admission never scans retained disks.
        delta = {
            "machines": machines,
            "disk_bytes": disk_bytes,
            "running": running,
            "cpus": cpus,
            "memory_bytes": memory_bytes,
        }
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        for key in ("", owner):
            await con.execute(
                insert(counters)
                .values(pool=self.pool, owner=key, **dict.fromkeys(_METRICS, 0))
                .on_conflict_do_nothing(index_elements=[counters.c.pool, counters.c.owner])
            )
            await con.execute(
                counters.update()
                .filter_by(pool=self.pool, owner=key)
                .values({name: counters.c[name] + value for name, value in delta.items()})
            )

    async def reserve(self, allocation, resources: ComputerResources, *, running=False):
        if not isinstance(resources, ComputerResources) or type(running) is not bool:
            raise TypeError("Expected provider-attested computer resources")
        owner = allocation.owner.key
        attrs = dict(pool=self.pool, owner=owner, **asdict(resources))
        async with self.engine.begin() as con:
            await self._lock(con)
            existing = (
                (
                    await con.execute(
                        sa.select(reservations).where(reservations.c.allocation == allocation.key)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing and any(existing[k] != v for k, v in attrs.items()):
                raise CapabilityUnavailable("The computer's admitted pool, principal or resources changed")
            if existing and (not running or existing["running"]):
                return
            count, disk, active, cpus, memory = await self._usage(con)
            owned, _, owner_active, _, _ = await self._usage(con, owner)
            q = self.limits
            if not existing and (
                count >= q.machines
                or owned >= q.machines_per_owner
                or disk + resources.disk_bytes > q.disk_bytes
            ):
                raise CapabilityUnavailable("Persistent computer capacity is exhausted")
            if running and (
                active >= q.running
                or owner_active >= q.running_per_owner
                or cpus + resources.cpus > q.cpus
                or memory + resources.memory_bytes > q.memory_bytes
            ):
                raise CapabilityUnavailable("Running computer capacity is exhausted")
            if existing:
                await con.execute(
                    reservations.update()
                    .where(reservations.c.allocation == allocation.key)
                    .values(running=True)
                )
            else:
                insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
                result = await con.execute(
                    insert(reservations)
                    .values(allocation=allocation.key, running=running, **attrs)
                    .on_conflict_do_nothing(index_elements=[reservations.c.allocation])
                    .returning(reservations.c.allocation)
                )
                if result.scalar_one_or_none() is None:
                    raise CapabilityUnavailable("The allocation is already bound to a different pool")

            await self._adjust(
                con,
                owner,
                machines=int(not existing),
                disk_bytes=resources.disk_bytes if not existing else 0,
                running=int(running),
                cpus=resources.cpus if running else 0,
                memory_bytes=resources.memory_bytes if running else 0,
            )

    async def stopped(self, allocation):
        """Trusted provider callback after an observed stop. The disk remains allocated."""
        async with self.engine.begin() as con:
            await self._lock(con)
            result = await con.execute(
                reservations.update()
                .where(
                    reservations.c.pool == self.pool,
                    reservations.c.allocation == allocation.key,
                    reservations.c.owner == allocation.owner.key,
                    reservations.c.running.is_(True),
                )
                .values(running=False)
                .returning(reservations.c.cpus, reservations.c.memory_bytes)
            )
            if released := result.one_or_none():
                await self._adjust(
                    con,
                    allocation.owner.key,
                    running=-1,
                    cpus=-released.cpus,
                    memory_bytes=-released.memory_bytes,
                )

    async def usage(self):
        """Operator aggregates. The host controls access to this method."""
        async with self.engine.connect() as con:
            values = await self._usage(con)
            return dict(zip(_METRICS, map(int, values), strict=True))
