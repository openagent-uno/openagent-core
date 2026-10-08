"""Shared repositories and dispatch for authorized automation management.

The scheduler algorithms and definition tables are retained. Product services
authorize changes and capture durable delegations in the same SQLite transaction.
"""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from functools import wraps
import importlib
import inspect
from pathlib import Path
import sqlite3
import json
from typing import Any, get_type_hints

_MODULES = {
    'scheduled_task': 'scheduler',
    'event': 'events_manager',
    'workflow': 'workflow_manager',
}
_active_connection: ContextVar[Any] = ContextVar('openagent_automation_transaction',default=None)


def repository_connection():
    active = _active_connection.get()
    if active is None:
        raise PermissionError('Automation data requires the authorized management service')
    return active[1]


def _module(kind):
    if kind not in _MODULES:
        raise ValueError('Unknown automation definition kind')
    return importlib.import_module('openagent_core.mcp.servers.'+_MODULES[kind]+'.server')


class _TransactionConnection:
    def __init__(self,connection): self._connection=connection
    def __getattr__(self,name): return getattr(self._connection,name)
    async def commit(self): pass
    async def rollback(self):
        raise RuntimeError('Abort the enclosing automation transaction')
    async def executescript(self,script):
        # sqlite3.executescript implicitly commits. Execute complete statements
        # instead so manager helper DDL cannot separate a definition and grant.
        statement=''
        for char in script:
            statement+=char
            if char==';' and sqlite3.complete_statement(statement):
                await self._connection.execute(statement)
                statement=''
        if statement.strip():
            await self._connection.execute(statement)


class AutomationRepository:
    def __init__(self,path: str | Path):
        self.path=Path(path).absolute()
        self._lock=asyncio.Lock()

    def definition_store(self,connection):
        """Full public storage API with borrowed transaction ownership.

        REST adapters retain fields that are intentionally absent from the
        model's shorter tool schema. The host still owns authorization/capture.
        """
        active=_active_connection.get()
        if active is None or active!=(self,connection):
            raise PermissionError('The connection must belong to this repository scope')
        from .engine import MemoryDB
        return MemoryDB.from_connection(connection,db_path=str(self.path))

    @asynccontextmanager
    async def session(self):
        """Preserve enqueue/commit/poll boundaries for execution operations."""
        current=_active_connection.get()
        if current is not None:
            if current[0] is not self:
                raise RuntimeError('Cannot mix automation repositories')
            yield current[1]
            return
        import aiosqlite
        async with aiosqlite.connect(self.path,timeout=5) as connection:
            connection.row_factory=aiosqlite.Row
            await connection.execute('PRAGMA foreign_keys=ON')
            token=_active_connection.set((self,connection))
            try:
                yield connection
            finally:
                _active_connection.reset(token)

    @asynccontextmanager
    async def transaction(self):
        current=_active_connection.get()
        if current is not None:
            if current[0] is not self:
                raise RuntimeError('Cannot mix automation repositories in one transaction')
            yield current[1]
            return
        import aiosqlite
        async with self._lock:
            async with aiosqlite.connect(self.path,timeout=5) as connection:
                connection.row_factory=aiosqlite.Row
                await connection.execute('PRAGMA foreign_keys=ON')
                await connection.execute('BEGIN IMMEDIATE')
                proxy=_TransactionConnection(connection)
                token=_active_connection.set((self,proxy))
                try:
                    yield proxy
                    await connection.commit()
                except BaseException:
                    await connection.rollback()
                    raise
                finally:
                    _active_connection.reset(token)

    async def call(self,kind: str,name: str,arguments: dict,context, *, atomic: bool = True):
        from .runtime import current_execution_context,current_runtime
        runtime=current_runtime()
        if runtime is None or current_execution_context()!=context:
            raise PermissionError('Automation repository requires the current verified execution context')
        module=_module(kind)
        allowed={tool.name for tool in module.mcp._tool_manager.list_tools()}
        if name not in allowed:
            raise LookupError('Unknown automation operation')
        function=getattr(module,name)
        async with (self.transaction() if atomic else self.session()):
            return await function(**arguments)


async def durable_module_references(repository: AutomationRepository,
                                    removed_modules: frozenset[str]):
    """Return active automation resources that depend on removed modules.

    This is a read-only preflight. Definitions remain owned by their modules;
    the runtime merely refuses an ambiguous hot deactivation until the host
    explicitly pauses or rewrites the named resources.
    """
    references: dict[str, list[str]] = {module_id: [] for module_id in removed_modules}
    async with repository.session() as connection:
        if removed_modules & {"workflows", "scheduler"}:
            cursor = await connection.execute(
                "SELECT id,action_kind,action_ref FROM events WHERE enabled=1 "
                "AND action_kind IN ('workflow','scheduled_task')"
            )
            for row in await cursor.fetchall():
                target = "workflows" if row["action_kind"] == "workflow" else "scheduler"
                if target in removed_modules:
                    references[target].append(
                        f"event:{row['id']}->{row['action_kind']}:{row['action_ref']}"
                    )
        if "scheduler" in removed_modules or "workflows" in removed_modules:
            cursor = await connection.execute(
                "SELECT id,workflow_id,node_id FROM workflow_schedules WHERE enabled=1"
            )
            for row in await cursor.fetchall():
                value = f"workflow_schedule:{row['id']}@{row['workflow_id']}:{row['node_id']}"
                if "scheduler" in removed_modules:
                    references["scheduler"].append(value)
                if "workflows" in removed_modules:
                    references["workflows"].append(value)
        if "mcp" in removed_modules:
            try:
                cursor = await connection.execute("SELECT name FROM mcps WHERE enabled=1")
                external_sources = {str(row["name"]) for row in await cursor.fetchall()}
            except sqlite3.OperationalError:
                external_sources = set()
            if external_sources:
                cursor = await connection.execute(
                    "SELECT id,graph_json FROM workflow_tasks WHERE enabled=1"
                )
                for row in await cursor.fetchall():
                    try:
                        graph = json.loads(row["graph_json"] or "{}")
                    except (TypeError, ValueError):
                        continue
                    for node in graph.get("nodes", ()) if isinstance(graph, dict) else ():
                        if not isinstance(node, dict):
                            continue
                        config = node.get("config") or {}
                        source = str(config.get("mcp_name") or "")
                        if node.get("type") == "mcp-tool" and source in external_sources:
                            references["mcp"].append(
                                f"workflow:{row['id']}/node:{node.get('id')}->{source}"
                            )
    return {key: tuple(dict.fromkeys(values)) for key, values in references.items() if values}


def build_automation_toolkit(kind: str):
    from .mcp._runtime import Toolkit
    from .runtime import current_execution_context,current_runtime
    module=_module(kind)
    wrappers=[]
    for tool in module.mcp._tool_manager.list_tools():
        function=getattr(module,tool.name)
        signature=inspect.signature(function,eval_str=True)
        def wrap(fn,signature):
            @wraps(fn)
            async def invoke(*args,**kwargs):
                runtime,context=current_runtime(),current_execution_context()
                service=getattr(getattr(runtime,'services',None),'automation_management',None)
                if service is None or context is None:
                    raise PermissionError('This host does not expose automation management')
                bound=signature.bind(*args,**kwargs);bound.apply_defaults()
                return await service.call(kind,fn.__name__,dict(bound.arguments),context)
            invoke.__signature__=signature
            invoke.__annotations__=get_type_hints(fn)
            return invoke
        wrappers.append(wrap(function,signature))

    # Historical definitions intentionally do not inherit an owner at startup.
    # Expose the host's explicit revision-review service beside each native
    # automation source so an authenticated App, Telegram or other live turn
    # can inspect and approve the exact persisted revision without resorting
    # to a disable/enable rewrite.  These are synthetic host operations rather
    # than SQLite manager functions: the host owns principal policy and grant
    # capture, while Core only supplies the portable tool surface.
    noun = {
        "scheduled_task": "scheduled_task",
        "workflow": "workflow",
        "event": "event",
    }[kind]

    async def review_automation_authorization(definition_id: str) -> dict[str, Any]:
        """Inspect one persisted automation revision before approving it.

        This is read-only. Use the returned full definition and digest to show
        the user exactly what would run. Historical definitions remain paused
        until the current authenticated user explicitly approves that digest.
        """
        runtime, context = current_runtime(), current_execution_context()
        service = getattr(getattr(runtime, "services", None), "automation_management", None)
        if service is None or context is None:
            raise PermissionError("This host does not expose automation management")
        return await service.review_authorization(kind, definition_id, context)

    review_automation_authorization.__name__ = f"review_{noun}_authorization"

    async def approve_automation_authorization(
        definition_id: str, digest: str,
    ) -> dict[str, Any]:
        """Approve exactly one previously reviewed automation revision.

        Call the matching review tool first and pass its exact digest. Only
        call this after the current authenticated user explicitly asks to
        activate this definition; existence or enabled state is not consent.
        A changed definition is rejected instead of approving newer behavior.
        """
        runtime, context = current_runtime(), current_execution_context()
        service = getattr(getattr(runtime, "services", None), "automation_management", None)
        if service is None or context is None:
            raise PermissionError("This host does not expose automation management")
        return await service.approve_authorization(
            kind, definition_id, digest, context,
        )

    approve_automation_authorization.__name__ = f"approve_{noun}_authorization"
    wrappers.extend((review_automation_authorization, approve_automation_authorization))
    return Toolkit(name=_MODULES[kind].replace('_','-'),tools=wrappers)
