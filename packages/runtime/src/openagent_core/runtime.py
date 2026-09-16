"""Explicit instance lifecycle and durable run admission; no product bootstrap."""
from __future__ import annotations
from contextlib import contextmanager, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
import asyncio
import copy

from .capabilities import CapabilityCatalog
from .code_execution import CodeExecutor
from .memory_access import MemoryAccess
from .contracts import (AgentExecutor, Authorizer, DelegationService, ExecutionContext,
    ResourceRef, RunEvent, RunRecord, RunRequest, RuntimeStore, ModelCatalog, PrincipalRef, require_authorized)

_current_context: ContextVar[ExecutionContext | None] = ContextVar('openagent_execution_context', default=None)
_current_runtime: ContextVar[Runtime | None] = ContextVar('openagent_runtime', default=None)
_current_run: ContextVar[str | None] = ContextVar('openagent_run_id', default=None)


def current_execution_context() -> ExecutionContext | None:
    return _current_context.get()


def current_runtime() -> Runtime | None:
    return _current_runtime.get()


def current_run_id() -> str | None:
    return _current_run.get()


@contextmanager
def execution_scope(runtime: Runtime, context: ExecutionContext, run_id: str):
    tokens = (_current_runtime.set(runtime), _current_context.set(context), _current_run.set(run_id))
    try:
        yield
    finally:
        _current_run.reset(tokens[2]); _current_context.reset(tokens[1]); _current_runtime.reset(tokens[0])


@contextmanager
def runtime_scope(runtime: Runtime):
    token = _current_runtime.set(runtime)
    try:
        yield
    finally:
        _current_runtime.reset(token)


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    agent_id: str
    workspace: Path
    enabled_modules: tuple[str, ...] = ()
    environment: tuple[tuple[str, str], ...] = ()
    child_concurrency: int = 16
    child_max_depth: int = 5
    child_chain_concurrency: int = 8

    def __post_init__(self) -> None:
        if not self.agent_id or not self.workspace.is_absolute():
            raise ValueError('Runtime requires an agent ID and an explicit absolute workspace')
        if self.child_concurrency < 1 or self.child_max_depth < 1 or self.child_chain_concurrency < 0:
            raise ValueError('Invalid child execution limits')


@dataclass(frozen=True, slots=True)
class RuntimeServices:
    store: RuntimeStore
    executor: AgentExecutor
    authorizer: Authorizer
    capabilities: CapabilityCatalog | None = None
    delegations: DelegationService | None = None
    owns_store: bool = True
    models: ModelCatalog | None = None
    user_sources: frozenset[str] = frozenset()
    catalog_management: Any | None = None
    automation_management: Any | None = None
    code_executor: CodeExecutor | None = None
    memory_access: MemoryAccess | None = None


class RuntimeModule(Protocol):
    async def start(self, runtime: Runtime) -> None: ...
    async def close(self) -> None: ...


class Runtime:
    def __init__(self, settings: RuntimeSettings, services: RuntimeServices,
                 modules: tuple[RuntimeModule, ...] = ()) -> None:
        self.settings = settings
        self.services = services
        self.modules = modules
        self.module_registries: dict[str, dict] = {}
        self.capabilities = services.capabilities or CapabilityCatalog(services.authorizer)
        self._started = False
        self._closing = False
        self._started_modules: list[RuntimeModule] = []
        self._tasks: dict[str, asyncio.Task] = {}
        self._sessions: dict[tuple[str,str], asyncio.Lock] = {}
        self._lifecycle = asyncio.Lock()

    async def start(self) -> None:
        async with self._lifecycle:
            if self._started:
                return
            if self._closing:
                raise RuntimeError('Closed runtimes cannot be restarted')
            try:
                if self.services.owns_store:
                    await self.services.store.start()
                    await self.services.store.recover()
                for module in self.modules:
                    # A partially started module owns rollback of its own
                    # resources; successfully started modules are closed here.
                    with runtime_scope(self):
                        await module.start(self)
                    self._started_modules.append(module)
            except BaseException:
                for module in reversed(self._started_modules):
                    with runtime_scope(self):
                        await module.close()
                self._started_modules.clear()
                if self.services.owns_store:
                    await self.services.store.close()
                raise
            self._started = True

    async def close(self) -> None:
        async with self._lifecycle:
            if self._closing:
                return
            self._closing = True
            tasks = tuple(self._tasks.values())
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            # A task cancelled before its first instruction cannot run finally.
            for run_id in tuple(self._tasks):
                await self.services.store.transition(run_id, 'interrupted', output={'reason':'runtime_closed'})
            self._tasks.clear()
            errors = []
            for module in reversed(self._started_modules):
                try:
                    with runtime_scope(self):
                        await module.close()
                except Exception as exc:
                    errors.append(exc)
            self._started_modules.clear()
            # The runtime owns its registry even when a lightweight executor
            # did not construct an Agent with its own shutdown hook.
            logging_state = self.module_registries.get('event-logging', {})
            handler = logging_state.pop('handler', None)
            if handler is not None:
                handler.close()
            if self.services.owns_store:
                await self.services.store.close()
            self._started = False
            if errors:
                raise ExceptionGroup('Runtime module shutdown failed',errors)

    def _admission(self) -> None:
        if not self._started or self._closing:
            raise RuntimeError('Runtime is not accepting work')

    async def _authorize(self, context: ExecutionContext, action: str, session_id: str, tenant_id: str) -> None:
        await self.authorize(context, action, ResourceRef('session', tenant_id, session_id), audience=context.audience)

    async def authorize(self, context: ExecutionContext, action: str, resource: ResourceRef, *, audience=()) -> None:
        """Revalidate delegation and current host policy at every operation."""
        if context.agent_id != self.settings.agent_id:
            raise PermissionError('Context belongs to a different agent runtime')
        if context.delegation_id:
            if self.services.delegations is None or not await self.services.delegations.validate(context):
                raise PermissionError('Execution delegation is missing or revoked')
        await require_authorized(self.services.authorizer,context,action,resource,audience=audience)

    @asynccontextmanager
    async def _credentials(self, context: ExecutionContext):
        """Optional host scope resolves ephemeral credentials from a delegation.

        The host owns refresh and teardown. Neither credentials nor their
        resolver are serialized into the execution context or run record.
        """
        scope = getattr(self.services.delegations, 'execution_scope', None)
        if context.delegation_id and scope is not None:
            async with scope(context):
                yield
        else:
            yield

    async def submit(self, request: RunRequest, context: ExecutionContext) -> RunRecord:
        return await self._submit(request,context,self.services.executor)

    async def execute_operation(self, request: RunRequest, context: ExecutionContext, executor: AgentExecutor) -> RunRecord:
        """Admit a trusted host operation through the same durable run service.

        The callback is host code, never wire input. Its versioned definition
        belongs in request.input and is protected by the request fingerprint.
        Acceptance/retry/recovery never reconstruct or replay the callback.
        Child agent runs use the runtime's ordinary agent executor.
        """
        if not context.deferred or not context.delegation_id:
            raise PermissionError('An automatic operation requires an explicit durable delegation')
        return await self._submit(request,context,executor)

    async def _submit(self, request: RunRequest, context: ExecutionContext, executor: AgentExecutor) -> RunRecord:
        self._admission()
        request = copy.deepcopy(request)
        if request.session_id != context.session_id:
            raise ValueError('Request and verified context must identify the same session')
        async with self._credentials(context):
            await self._authorize(context,'run.submit',request.session_id,context.tenant_id)
            if request.steer_run_id is not None:
                await self._steering_target(request,context)
        record, created = await self.services.store.accept(request,context)
        if created:
            self._tasks[record.run_id] = asyncio.create_task(self._execute(request,context,executor),name='openagent-run-'+record.run_id)
        return record

    async def _steering_target(self, request: RunRequest, context: ExecutionContext) -> RunRecord:
        target = await self.get_run(request.steer_run_id,context)
        if target.session_id != request.session_id or target.tenant_id != context.tenant_id:
            raise PermissionError('Steering target belongs to a different session')
        await self._authorize(context,'run.cancel',target.session_id,target.tenant_id)
        return target

    async def _execute(self, request: RunRequest, context: ExecutionContext, executor: AgentExecutor) -> None:
        try:
            with execution_scope(self,context,request.run_id):
                async with self._credentials(context), asyncio.timeout(request.deadline_seconds):
                    if request.steer_run_id is not None:
                        # The new message already has a durable identity and its
                        # own verified authority. Never merge into the target's
                        # execution context or select a newer run on retry.
                        target = await self._steering_target(request,context)
                        await self.services.store.append_event(request.run_id,'run.steering',{'target_run_id':target.run_id})
                        await self.cancel(target.run_id,context)
                    lock=self._sessions.setdefault((context.tenant_id,context.session_id),asyncio.Lock())
                    # Different authors never steer/coalesce into another
                    # principal's active execution or borrow its credentials.
                    async with lock:
                        await self._authorize(context,'run.execute',request.session_id,context.tenant_id)
                        record=await self.services.store.get(request.run_id)
                        if record.cancel_requested:
                            await self.services.store.transition(request.run_id,'cancelled')
                            return
                        await self.services.store.transition(request.run_id,'running')
                        result=await executor.execute(request,context,self)
                        await self._authorize(context,'run.publish',request.session_id,context.tenant_id)
                        # Cancellation is an intention until execution ends.
                        record=await self.services.store.get(request.run_id)
                        await self.services.store.transition(request.run_id,'cancelled' if record.cancel_requested else 'success',
                                                             output=None if record.cancel_requested else result)
        except TimeoutError:
            await self.services.store.transition(request.run_id,'timed_out')
        except asyncio.CancelledError:
            await self.services.store.transition(request.run_id,'interrupted' if self._closing else 'cancelled')
        except Exception as exc:
            # Details may include secrets or private provider diagnostics.
            await self.services.store.transition(request.run_id,'failed',output={'error_type':type(exc).__name__})
        finally:
            self._tasks.pop(request.run_id,None)

    async def get_run(self, run_id: str, context: ExecutionContext) -> RunRecord:
        record=await self.services.store.get(run_id)
        if record is None:
            raise LookupError(run_id)
        await self._authorize(context,'run.read',record.session_id,record.tenant_id)
        return record

    async def events(self, run_id: str, after: int, context: ExecutionContext) -> tuple[RunEvent,...]:
        record=await self.get_run(run_id,context)
        await self._authorize(context,'run.replay',record.session_id,record.tenant_id)
        return await self.services.store.events(run_id,after)

    async def cancel(self, run_id: str, context: ExecutionContext, *, reserve: bool = False) -> RunRecord:
        if reserve and await self.services.store.get(run_id) is None:
            self._admission()
            await self._authorize(context,'run.cancel',context.session_id,context.tenant_id)
            reserved = await self.services.store.reserve_cancel(run_id,context)
            if reserved.terminal:
                return reserved
        record=await self.get_run(run_id,context)
        await self._authorize(context,'run.cancel',record.session_id,record.tenant_id)
        if record.terminal:
            return record
        record=await self.services.store.request_cancel(run_id)
        task=self._tasks.get(run_id)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task,return_exceptions=True)
            # Includes cancellation before the coroutine starts.
            record=await self.services.store.transition(run_id,'cancelled')
            self._tasks.pop(run_id,None)
        return record

    async def wait(self, run_id: str, context: ExecutionContext) -> RunRecord:
        await self.get_run(run_id,context)
        task=self._tasks.get(run_id)
        if task is not None:
            # An observer disconnect does not cancel an accepted execution.
            await asyncio.shield(task)
        return await self.get_run(run_id,context)

    async def spawn(self, request: RunRequest, context: ExecutionContext, *, deferred: bool = False) -> RunRecord:
        parent_id=current_run_id()
        if parent_id is None:
            raise PermissionError("A child execution requires a live authorized parent run")
        await self._authorize(context,"run.delegate",context.session_id,context.tenant_id)
        author=PrincipalRef(context.authority.authority,context.tenant_id,self.settings.agent_id,"agent")
        child=context.child(session_id=request.session_id,run_id=parent_id,agent=author,deferred=deferred)
        record=await self.submit(request,child)
        await self.services.store.append_event(parent_id,"run.child",{"child_session_id":record.session_id,"child_run_id":record.run_id})
        try:
            return await self.wait(record.run_id,child)
        except asyncio.CancelledError:
            await self.services.store.request_cancel(record.run_id)
            task=self._tasks.get(record.run_id)
            if task is not None:
                task.cancel()
                await asyncio.gather(task,return_exceptions=True)
            await self.services.store.transition(record.run_id,"cancelled")
            raise

    async def children(self, run_id: str, context: ExecutionContext) -> tuple[RunRecord,...]:
        await self.get_run(run_id,context)
        records=await self.services.store.children(run_id)
        return tuple([await self.get_run(r.run_id,context) for r in records])
