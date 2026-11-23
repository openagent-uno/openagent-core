"""Host-facing task computer allocations and generation-fenced remote catalog access."""

from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
from dataclasses import asdict, dataclass, replace
import hashlib
import math
import re
import time
from typing import Protocol
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.contracts import (
    CATALOG_PARTITION_PREFIX,
    ExecutionContext,
    IdempotencyConflict,
    PrincipalRef,
    ResourceRef,
    canonical_json,
    require_authorized,
)
from openagent_core.remote_mcp import RemoteMcpRegistry, catalog as remote_catalog


schema = sa.MetaData()
tasks = sa.Table(
    "oa_computer_task",
    schema,
    sa.Column("allocation", sa.String(64), primary_key=True),
    sa.Column("tenant", sa.String(200), nullable=False),
    sa.Column("owner", sa.Text, nullable=False),
    sa.Column("agent", sa.String(200), nullable=False),
    sa.Column("project", sa.String(200), nullable=False),
    sa.Column("task", sa.String(200), nullable=False),
    sa.Column("base_version", sa.String(200), nullable=False),
    sa.Column("machine_id", sa.String(256), unique=True),
    sa.Column("host_id", sa.String(256), unique=True),
    sa.Column("generation", sa.BigInteger),
    sa.Column("epoch", sa.BigInteger, nullable=False),
    sa.Column("expires_at", sa.Float),
    sa.Column("state", sa.String(16), nullable=False),
)

_PARTITION = re.compile(r"computer-([a-f0-9]{64})-([1-9][0-9]{0,18})")
_HOST_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,255}")


@dataclass(frozen=True)
class ComputerTask:
    project: str
    task: str
    base_version: str

    def __post_init__(self):
        if any(
            not isinstance(value, str) or not value.strip() or len(value.encode()) > 200
            for value in (self.project, self.task, self.base_version)
        ):
            raise ValueError("A computer task requires bounded project, task and base version IDs")


@dataclass(frozen=True)
class ComputerAllocation:
    key: str
    owner: PrincipalRef
    agent: str
    task: ComputerTask

    @classmethod
    def for_context(cls, context, task):
        if not isinstance(context, ExecutionContext) or not isinstance(task, ComputerTask):
            raise TypeError("An authenticated context and computer task are required")
        if any(len(value.encode()) > 200 for value in (context.tenant_id, context.agent_id)):
            raise ValueError("Invalid tenant or agent identity")
        identity = (context.initiator.key, context.agent_id, task.project, task.task)
        return cls(
            hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
            context.initiator,
            context.agent_id,
            task,
        )


@dataclass(frozen=True)
class ComputerConnection:
    machine_id: str
    host_id: str
    generation: int
    expires_at: float


@dataclass(frozen=True)
class ComputerLease:
    allocation: str
    epoch: int
    machine_id: str
    host_id: str
    generation: int

    @property
    def partition(self):
        return f"computer-{self.allocation}-{self.epoch}"


class TaskComputerProvider(Protocol):
    async def ensure(self, allocation: ComputerAllocation) -> str:
        """Provision or reconcile exactly one isolated machine for allocation.key."""
        ...

    async def connection(self, allocation: ComputerAllocation, machine_id: str) -> ComputerConnection:
        """Return an authenticated current registration for this exact allocation and machine."""
        ...


@dataclass
class _Mount:
    registry: RemoteMcpRegistry
    ready: asyncio.Task
    users: int = 0


class TaskComputerBroker:
    """Compose an ACL, idempotent infrastructure provider and exact registered-device transport."""

    def __init__(
        self,
        engine,
        authorizer,
        provider: TaskComputerProvider,
        transport,
        *,
        delegations=None,
        clock=time.time,
        max_lease_seconds=300,
    ):
        if engine.dialect.name not in {"sqlite", "postgresql"}:
            raise ValueError("Computer task allocations require SQLite or PostgreSQL")
        if not math.isfinite(max_lease_seconds) or max_lease_seconds <= 0:
            raise ValueError("A positive finite maximum lease duration is required")
        self.engine, self.authorizer, self.provider, self.transport = (
            engine,
            authorizer,
            provider,
            transport,
        )
        self.clock, self.max_lease, self.delegations = clock, max_lease_seconds, delegations
        self._mounts = {}

    async def initialize(self):
        async with self.engine.begin() as con:
            await con.run_sync(schema.create_all)

    async def _authorize(self, context, key, action, *, base=None):
        if context.deferred and (self.delegations is None or not await self.delegations.validate(context)):
            raise PermissionError("The computer task requires a live delegation")
        await require_authorized(
            self.authorizer,
            context,
            action,
            ResourceRef("computer-task", context.tenant_id, key),
            audience=context.audience,
        )
        if base is not None:
            await self._authorize_base(context, *base)

    async def _authorize_base(self, context, project, version):
        await require_authorized(
            self.authorizer,
            context,
            "computer.base.read",
            ResourceRef("computer-project-version", context.tenant_id, canonical_json([project, version])),
            audience=context.audience,
        )

    async def _row(self, key, con=None):
        query = sa.select(tasks).where(tasks.c.allocation == key)
        if con is not None:
            return (await con.execute(query)).mappings().one()
        async with self.engine.connect() as reader:
            return (await reader.execute(query)).mappings().one_or_none()

    async def provision(self, context, task: ComputerTask):
        allocation = ComputerAllocation.for_context(context, task)
        base = (task.project, task.base_version)
        await self._authorize(context, allocation.key, "computer.provision", base=base)
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        async with self.engine.begin() as con:
            await con.execute(
                insert(tasks)
                .values(
                    allocation=allocation.key,
                    tenant=context.tenant_id,
                    owner=context.initiator.key,
                    agent=context.agent_id,
                    **asdict(task),
                    epoch=0,
                    state="pending",
                )
                .on_conflict_do_nothing(index_elements=[tasks.c.allocation])
            )
            row = await self._row(allocation.key, con)
        if row["base_version"] != task.base_version:
            raise IdempotencyConflict("A task already owns another immutable base version")
        if row["state"] == "revoked":
            raise PermissionError("The computer assignment was revoked")
        if row["machine_id"] is not None:
            await self._authorize(context, allocation.key, "computer.provision", base=base)
            return row["machine_id"]
        # The provider must use the same durable key after a lost response or
        # simultaneous caller. Do not keep a database transaction across IO.
        await self._authorize(context, allocation.key, "computer.provision", base=base)
        machine = await self.provider.ensure(allocation)
        if not isinstance(machine, str) or not machine or len(machine.encode()) > 256:
            raise ValueError("Invalid provisioned machine identity")
        async with self.engine.begin() as con:
            await con.execute(
                tasks.update()
                .where(tasks.c.allocation == allocation.key, tasks.c.state == "pending")
                .values(machine_id=machine, state="offline")
            )
            row = await self._row(allocation.key, con)
            await self._authorize(context, allocation.key, "computer.provision", base=base)
            if row["state"] == "revoked" or row["machine_id"] != machine:
                raise CapabilityUnavailable("The exact computer allocation changed during provisioning")
        return machine

    async def current_lease(self, context, task: ComputerTask):
        """Resolve an existing assignment without booting, renewing or replacing it."""
        if context.catalog_partition is not None or context.capabilities:
            raise PermissionError("Resolving a task requires the original verified context")
        allocation = ComputerAllocation.for_context(context, task)
        await self._authorize(
            context, allocation.key, "computer.use", base=(task.project, task.base_version)
        )
        row = await self._row(allocation.key)
        if not row or row["base_version"] != task.base_version or row["state"] != "online":
            raise CapabilityUnavailable("The current task computer is unavailable")
        candidate = ComputerLease(
            allocation.key, row["epoch"], row["machine_id"], row["host_id"], row["generation"]
        )
        bound = replace(context, scopes=context.scopes + (CATALOG_PARTITION_PREFIX + candidate.partition,))
        return await self._lease(bound, "computer.use")

    async def authorize_viewer_action(self, bound, lease, *, capture):
        """A product may grant screen capture independently from direct input."""
        action = "computer.viewer.capture" if capture else "computer.viewer.input"
        if await self._lease(bound, action) != lease:
            raise CapabilityUnavailable("The viewer assignment is no longer current")

    def viewer_target_id(self, catalog, bound, descriptor):
        """Project an exact live remote tool into a portable, non-authorizing viewer handle."""
        mount = self._mounts.get((catalog, bound.coalescing_key))
        source = mount.registry.sources.get(descriptor.source_id) if mount else None
        if source is None:
            raise CapabilityUnavailable("The desktop does not belong to this mounted computer")
        # owner includes tenant, principals, session, audience, allocation and
        # epoch. identity includes the remote device, generation and MCP server.
        identity = (
            source.owner,
            source.identity,
            source.row["version"],
            source.row["catalog_digest"],
            descriptor.name,
        )
        return hashlib.sha256(canonical_json(identity).encode()).hexdigest()

    async def attach(self, context, task: ComputerTask):
        """Resolve only the provider-attested registration. Same-generation refresh preserves references."""
        allocation = ComputerAllocation.for_context(context, task)
        key = allocation.key
        base = (task.project, task.base_version)
        await self._authorize(context, key, "computer.attach", base=base)
        row = await self._row(key)
        if not row or row["base_version"] != task.base_version or row["state"] not in {"online", "offline"}:
            raise CapabilityUnavailable("The task has no eligible computer assignment")
        reconnect = getattr(self.provider, "reconnect", None)
        if callable(reconnect) and row["state"] == "online" and row["expires_at"] <= self.clock():
            # Fence this exact expired generation before rotating the transport.
            # A concurrent healthy refresh or revocation wins its own CAS.
            async with self.engine.begin() as con:
                await self._authorize(context, key, "computer.attach", base=base)
                await con.execute(
                    tasks.update()
                    .where(
                        tasks.c.allocation == key,
                        tasks.c.epoch == row["epoch"],
                        tasks.c.state == "online",
                        tasks.c.expires_at <= self.clock(),
                    )
                    .values(state="offline", epoch=tasks.c.epoch + 1)
                )
                row = await self._row(key, con)
        if row["state"] == "revoked":
            raise CapabilityUnavailable("The task assignment was revoked")
        if callable(reconnect) and row["state"] == "offline" and row["generation"] is not None:
            conn = await reconnect(allocation, row["machine_id"], row["generation"])
        else:
            conn = await self.provider.connection(allocation, row["machine_id"])
        now = self.clock()
        if (
            not isinstance(conn, ComputerConnection)
            or conn.machine_id != row["machine_id"]
            or not isinstance(conn.host_id, str)
            or not _HOST_ID.fullmatch(conn.host_id)
            or type(conn.generation) is not int
            or not 1 <= conn.generation < 2**63
            or type(conn.expires_at) not in (int, float)
            or not math.isfinite(conn.expires_at)
            or not now < conn.expires_at <= now + self.max_lease
        ):
            raise CapabilityUnavailable("No valid connection to the assigned computer")
        same = (conn.host_id, conn.generation) == (row["host_id"], row["generation"])
        if same and (row["state"] != "online" or row["expires_at"] <= now):
            raise CapabilityUnavailable("An expired or disconnected registration needs a new generation")
        if row["host_id"] is not None and conn.host_id != row["host_id"]:
            raise CapabilityUnavailable("Enrollment identity changed for the assigned machine")
        if row["generation"] is not None and not same and conn.generation <= row["generation"]:
            raise CapabilityUnavailable("The provider returned a stale computer generation")
        epoch = row["epoch"] if same else row["epoch"] + 1
        async with self.engine.begin() as con:
            await self._authorize(context, key, "computer.attach", base=base)
            if conn.expires_at <= self.clock():
                raise CapabilityUnavailable("The connection expired during attachment")
            criteria = [
                tasks.c.allocation == key,
                tasks.c.epoch == row["epoch"],
                tasks.c.state == row["state"],
            ]
            if same:
                criteria.append(tasks.c.expires_at > self.clock())
            result = await con.execute(
                tasks.update()
                .where(*criteria)
                .values(
                    epoch=epoch,
                    state="online",
                    host_id=conn.host_id,
                    generation=conn.generation,
                    expires_at=sa.case(
                        (tasks.c.expires_at > conn.expires_at, tasks.c.expires_at), else_=conn.expires_at
                    )
                    if same
                    else conn.expires_at,
                )
            )
            if result.rowcount != 1:
                latest = await self._row(key, con)
                if not (
                    latest["state"] == "online"
                    and latest["epoch"] == epoch
                    and latest["host_id"] == conn.host_id
                    and latest["generation"] == conn.generation
                    and latest["expires_at"] > self.clock()
                ):
                    raise CapabilityUnavailable("The computer changed during attachment")
        return ComputerLease(key, epoch, conn.machine_id, conn.host_id, conn.generation)

    async def disconnected(self, lease: ComputerLease):
        """Trusted registration-loss callback. Old disconnects cannot affect a replacement."""
        async with self.engine.begin() as con:
            result = await con.execute(
                tasks.update()
                .filter_by(
                    allocation=lease.allocation,
                    state="online",
                    epoch=lease.epoch,
                    machine_id=lease.machine_id,
                    host_id=lease.host_id,
                    generation=lease.generation,
                )
                .values(state="offline", epoch=tasks.c.epoch + 1)
            )
            return result.rowcount == 1

    async def revoke(self, context, task):
        allocation = ComputerAllocation.for_context(context, task)
        await self._authorize(context, allocation.key, "computer.revoke")
        # Include a tombstone even if revocation wins a race with first provision.
        insert = pg_insert if self.engine.dialect.name == "postgresql" else sqlite_insert
        async with self.engine.begin() as con:
            stmt = insert(tasks).values(
                allocation=allocation.key,
                tenant=context.tenant_id,
                owner=context.initiator.key,
                agent=context.agent_id,
                **asdict(task),
                epoch=1,
                state="revoked",
            )
            await con.execute(
                stmt.on_conflict_do_update(
                    index_elements=[tasks.c.allocation],
                    where=tasks.c.state != "revoked",
                    set_={"state": "revoked", "epoch": tasks.c.epoch + 1},
                )
            )
            row = await self._row(allocation.key, con)
            if row["base_version"] != task.base_version:
                raise IdempotencyConflict("A task already owns another immutable base version")
            await self._authorize(context, allocation.key, "computer.revoke")
        fence = getattr(self.provider, "fence", None)
        if callable(fence):
            # Commit the admission fence first. Redelivery retries a provider
            # shutdown interrupted after the durable revocation.
            await fence(allocation)

    async def checkpoint(self, context, lease, request_id):
        """Freeze a reviewable diff of the exact mounted assignment."""
        if (
            not isinstance(request_id, str)
            or not 0 < len(request_id.encode()) <= 200
            or await self._lease(context, "computer.checkpoint") != lease
        ):
            raise CapabilityUnavailable("A mounted task and stable checkpoint request ID are required")
        result = await self.provider.checkpoint(lease, request_id)
        await self._lease(context, "computer.publish")
        return result

    async def read_checkpoint(self, context, task, request_id):
        """Return a saved authorized checkpoint, including after the VM stops."""
        if not isinstance(request_id, str) or not 0 < len(request_id.encode()) <= 200:
            raise ValueError("Expected a stable checkpoint request ID")
        allocation = ComputerAllocation.for_context(context, task)
        await self._authorize(
            context, allocation.key, "computer.review", base=(task.project, task.base_version)
        )
        row = await self._row(allocation.key)
        if not row or row["base_version"] != task.base_version:
            raise CapabilityUnavailable("Computer task is unavailable")
        result = await self.provider.read_checkpoint(allocation, request_id)
        await self._authorize(
            context, allocation.key, "computer.review", base=(task.project, task.base_version)
        )
        return result

    async def _lease(self, context, action):
        partition = context.catalog_partition or ""
        match = _PARTITION.fullmatch(partition)
        if match is None or context.capabilities:
            raise CapabilityUnavailable("A computer-only execution is required")
        key, epoch = match.groups()
        await self._authorize(context, key, action)
        row = await self._row(key)
        if (
            not row
            or row["owner"] != context.initiator.key
            or row["tenant"] != context.tenant_id
            or row["agent"] != context.agent_id
            or row["epoch"] != int(epoch)
            or row["state"] != "online"
            or row["expires_at"] <= self.clock()
        ):
            raise CapabilityUnavailable("The exact computer lease is unavailable")
        await self._authorize_base(context, row["project"], row["base_version"])
        return ComputerLease(key, int(epoch), row["machine_id"], row["host_id"], row["generation"])

    @asynccontextmanager
    async def tools(self, capability_catalog, context, lease):
        """Mount only this task's remote sources before Runtime.submit, through completion."""
        if context.catalog_partition is not None or context.capabilities:
            raise PermissionError("The turn already carries another computer or capability lease")
        bound = replace(context, scopes=context.scopes + (CATALOG_PARTITION_PREFIX + lease.partition,))
        if await self._lease(bound, "computer.use") != lease:
            raise CapabilityUnavailable("Computer assignment does not match this context")
        key = (capability_catalog, bound.coalescing_key)
        mount = self._mounts.get(key)
        if mount is None:
            registry = RemoteMcpRegistry(
                capability_catalog, self, source_prefix="computer_" + uuid4().hex, partitioned=True
            )
            mount = self._mounts[key] = _Mount(registry, asyncio.create_task(registry.synchronize(bound)))
        mount.users += 1
        try:
            topology = await asyncio.shield(mount.ready)
            if not topology.available or not topology.complete or not topology.rows:
                raise CapabilityUnavailable("The assigned computer has no complete, live catalog")
            yield bound
        finally:
            mount.users -= 1
            if mount.users == 0:
                self._mounts.pop(key)
                mount.ready.cancel()
                await asyncio.gather(mount.ready, return_exceptions=True)
                for source in mount.registry.sources:
                    capability_catalog.revoke(source)

    async def catalog(self, context, selectors):
        lease = await self._lease(context, "computer.use")
        exact = {"host_id": lease.host_id, "generation": lease.generation}
        if any(k in selectors and selectors[k] != v for k, v in exact.items()):
            raise CapabilityUnavailable("A different computer cannot satisfy this request")
        topology = await remote_catalog(self.transport, context, {**selectors, **exact})
        await self._lease(context, "computer.publish")
        if not topology.available:
            return None
        return {
            "servers": [r for r in topology.rows if all(r[k] == v for k, v in exact.items())],
            "truncated": not topology.complete,
        }

    async def _dispatch(self, context, scope, invoke, *args):
        await self.admit_target(context, scope.get("host_id"), scope.get("generation"))
        await self._lease(context, "computer.publish")
        result = await invoke(context, scope, *args)
        await self._lease(context, "computer.publish")
        return result

    async def admit_target(self, context, host_id, generation):
        """The receiving host calls this with authenticated context and its own current enrollment."""
        lease = await self._lease(context, "computer.use")
        if host_id != lease.host_id or type(generation) is not int or generation != lease.generation:
            raise CapabilityUnavailable("A different computer cannot satisfy this request")
        return lease

    async def call(self, context, scope, name, arguments):
        return await self._dispatch(context, scope, self.transport.call, name, arguments)

    async def read(self, context, scope, uri):
        return await self._dispatch(context, scope, self.transport.read, uri)
