"""Explicit instance lifecycle and durable run admission; no product bootstrap."""
from __future__ import annotations
from contextlib import contextmanager, asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
import asyncio
import copy
import inspect

from .capabilities import CapabilityCatalog
from .code_execution import CodeExecutor
from .memory_access import MemoryAccess
from .modules import (CapabilityContribution, ModuleCatalog,
    ModuleContext, ModuleContribution, ModuleReconfigurationError, ModuleReferenceConflict,
    ModuleStatus, ReconfigureReceipt, RuntimeProfile, ServiceRegistry)
from .contracts import (AgentExecutor, Authorizer, DelegationService, ExecutionContext,
    ResourceRef, RunEvent, RunRecord, RunRequest, RuntimeStore, SessionStore,
    ModelCatalog, PrincipalRef, require_authorized)

_UNSET = object()

_current_context: ContextVar[ExecutionContext | None] = ContextVar('openagent_execution_context', default=None)
_current_runtime: ContextVar[Runtime | None] = ContextVar('openagent_runtime', default=None)
_current_run: ContextVar[str | None] = ContextVar('openagent_run_id', default=None)
_current_module_generation: ContextVar[int | None] = ContextVar('openagent_module_generation', default=None)
_current_capability_revision: ContextVar[int | None] = ContextVar('openagent_capability_revision', default=None)


def current_execution_context() -> ExecutionContext | None:
    return _current_context.get()


def current_runtime() -> Runtime | None:
    return _current_runtime.get()


def current_run_id() -> str | None:
    return _current_run.get()


def current_module_generation() -> int | None:
    return _current_module_generation.get()


def current_capability_revision() -> int | None:
    return _current_capability_revision.get()


@contextmanager
def execution_scope(runtime: Runtime, context: ExecutionContext, run_id: str, *,
                    module_generation: int | None = None,
                    capability_revision: int | None = None):
    tokens = (_current_runtime.set(runtime), _current_context.set(context), _current_run.set(run_id),
              _current_module_generation.set(module_generation),
              _current_capability_revision.set(capability_revision))
    try:
        yield
    finally:
        _current_capability_revision.reset(tokens[4])
        _current_module_generation.reset(tokens[3])
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
    environment: tuple[tuple[str, str], ...] = ()
    child_concurrency: int = 16
    child_max_depth: int = 5
    child_chain_concurrency: int = 8

    def __post_init__(self) -> None:
        if not self.agent_id or not self.workspace.is_absolute():
            raise ValueError('Runtime requires an agent ID and an explicit absolute workspace')
        if self.child_concurrency < 1 or self.child_max_depth < 1 or self.child_chain_concurrency < 0:
            raise ValueError('Invalid child execution limits')


@dataclass(slots=True, init=False)
class RuntimeServices:
    """Kernel services plus one typed registry for optional services."""
    store: RuntimeStore
    executor: AgentExecutor
    authorizer: Authorizer
    capabilities: CapabilityCatalog | None
    owns_store: bool
    registry: ServiceRegistry

    def __init__(self, store: RuntimeStore, executor: AgentExecutor, authorizer: Authorizer,
                 capabilities: CapabilityCatalog | None = None, *, owns_store: bool = True,
                 registry: ServiceRegistry | None = None, delegations: Any = _UNSET,
                 models: Any = _UNSET, user_sources: Any = _UNSET,
                 catalog_management: Any = _UNSET, automation_management: Any = _UNSET,
                 code_executor: Any = _UNSET, memory_access: Any = _UNSET) -> None:
        self.store, self.executor, self.authorizer = store, executor, authorizer
        self.capabilities, self.owns_store = capabilities, owns_store
        self.registry = registry or ServiceRegistry()
        for key, value in (
            (DelegationService, delegations), ("delegations", delegations),
            (ModelCatalog, models), ("models", models),
            (CodeExecutor, code_executor), ("code_executor", code_executor),
            (MemoryAccess, memory_access), ("memory_access", memory_access),
            ("catalog_management", catalog_management),
            ("automation_management", automation_management),
            ("user_sources", user_sources),
        ):
            if value is _UNSET:
                continue
            if value is None:
                self.registry.unbind(key)
            else:
                self.registry.bind(key, value, replace=self.registry.has(key))

    # Read-only beta compatibility. New code calls Runtime.service(key).
    def _legacy(self, key, default=None):
        return self.registry.optional(key, default)

    @property
    def delegations(self): return self._legacy(DelegationService)
    @delegations.setter
    def delegations(self, value): self._set_legacy(DelegationService, "delegations", value)
    @property
    def models(self): return self._legacy(ModelCatalog)
    @models.setter
    def models(self, value): self._set_legacy(ModelCatalog, "models", value)
    @property
    def user_sources(self): return self._legacy("user_sources", frozenset())
    @user_sources.setter
    def user_sources(self, value): self._set_legacy("user_sources", None, value)
    @property
    def catalog_management(self): return self._legacy("catalog_management")
    @catalog_management.setter
    def catalog_management(self, value): self._set_legacy("catalog_management", None, value)
    @property
    def automation_management(self): return self._legacy("automation_management")
    @automation_management.setter
    def automation_management(self, value): self._set_legacy("automation_management", None, value)
    @property
    def code_executor(self): return self._legacy(CodeExecutor)
    @code_executor.setter
    def code_executor(self, value): self._set_legacy(CodeExecutor, "code_executor", value)
    @property
    def memory_access(self): return self._legacy(MemoryAccess)
    @memory_access.setter
    def memory_access(self, value): self._set_legacy(MemoryAccess, "memory_access", value)

    def _set_legacy(self, key: object, alias: object | None, value: Any) -> None:
        for current in (key, alias):
            if current is None:
                continue
            if value is None:
                self.registry.unbind(current)
            else:
                self.registry.bind(current, value, replace=self.registry.has(current))


class RuntimeModule(Protocol):
    async def start(self, runtime: Runtime) -> None: ...
    async def close(self) -> None: ...


@dataclass(slots=True)
class _ActiveGraph:
    profile: RuntimeProfile
    services: ServiceRegistry
    instances: list[tuple[Any, Any, ModuleContribution]] = field(default_factory=list)
    prompt_blocks: tuple[Any, ...] = ()
    routes: tuple[Any, ...] = ()
    health_checks: tuple[Any, ...] = ()
    search_providers: tuple[Any, ...] = ()
    workers: tuple[Any, ...] = ()
    event_ingresses: tuple[Any, ...] = ()
    indices: tuple[Any, ...] = ()
    components_started: bool = False
    leases: int = 0
    draining: bool = False
    drained: asyncio.Event = field(default_factory=asyncio.Event)

    def acquire(self) -> None:
        if self.draining:
            raise RuntimeError("The selected module graph is draining")
        self.leases += 1
        self.drained.clear()

    def release(self) -> None:
        if self.leases < 1:
            return
        self.leases -= 1
        if self.leases == 0:
            self.drained.set()


class Runtime:
    def __init__(self, settings: RuntimeSettings, services: RuntimeServices,
                 modules: tuple[RuntimeModule, ...] = (), *,
                 profile: RuntimeProfile | None = None,
                 module_catalog: ModuleCatalog | None = None) -> None:
        self.settings = settings
        self.services = services
        self.modules = modules
        self.module_catalog = module_catalog or ModuleCatalog()
        self._initial_profile = profile or RuntimeProfile()
        self.module_registries: dict[str, dict] = {}
        self.capabilities = services.capabilities or CapabilityCatalog(services.authorizer)
        self._host_services = self._build_host_registry()
        self._component_references: dict[int, int] = {}
        self._component_lock = asyncio.Lock()
        self._active_graph: _ActiveGraph | None = None
        self._retired_graphs: dict[int, _ActiveGraph] = {}
        self._run_graphs: dict[str, _ActiveGraph] = {}
        self._run_capability_revisions: dict[str, int] = {}
        self._applied_module_migrations: set[tuple[str, int]] = set()
        self._started = False
        self._closing = False
        self._started_modules: list[RuntimeModule] = []
        self._tasks: dict[str, asyncio.Task] = {}
        self._sessions: dict[tuple[str,str], asyncio.Lock] = {}
        self._lifecycle = asyncio.Lock()

    def _build_host_registry(self) -> ServiceRegistry:
        registry = self.services.registry.clone()
        for key, value in (
            (RuntimeStore, self.services.store), ("runtime.store", self.services.store),
            (SessionStore, self.services.store), ("sessions.store", self.services.store),
            (AgentExecutor, self.services.executor), ("runtime.executor", self.services.executor),
            (Authorizer, self.services.authorizer), ("runtime.authorizer", self.services.authorizer),
            (CapabilityCatalog, self.capabilities), ("runtime.capabilities", self.capabilities),
        ):
            if value is not None and not registry.has(key):
                registry.bind(key, value)
        return registry

    @property
    def profile(self) -> RuntimeProfile:
        graph = self._active_graph
        return graph.profile if graph is not None else self._initial_profile

    @property
    def uses_descriptor_profile(self) -> bool:
        return True

    @property
    def enabled_module_ids(self) -> frozenset[str]:
        return self.profile.module_ids

    @property
    def prompt_blocks(self) -> tuple[Any, ...]:
        graph = self._graph_for_current_run()
        return graph.prompt_blocks if graph is not None else ()

    @property
    def routes(self) -> tuple[Any, ...]:
        graph = self._active_graph
        return graph.routes if graph is not None else ()

    def service(self, key: object, default: Any = None) -> Any:
        graph = self._graph_for_current_run() or self._active_graph
        if graph is not None and graph.services.owner(key) not in {None, "host"}:
            return graph.services.optional(key, default)
        # Host bindings are live policy/configuration state.  They may be
        # replaced or revoked while a run is active and must be revalidated.
        return self.services.registry.optional(key, default)

    def services_for(self, key: object) -> tuple[Any, ...]:
        graph = self._graph_for_current_run() or self._active_graph
        if graph is None:
            return self.services.registry.all(key)
        module_values = tuple(value for value in graph.services.all(key)
                              if value not in self._host_services.all(key))
        return (*self.services.registry.all(key), *module_values)

    def _graph_for_current_run(self) -> _ActiveGraph | None:
        run_id = current_run_id()
        if run_id is not None:
            return self._run_graphs.get(run_id)
        generation = current_module_generation()
        if generation is not None:
            if self._active_graph is not None and self._active_graph.profile.generation == generation:
                return self._active_graph
            return self._retired_graphs.get(generation)
        return self._active_graph

    async def _prepare_graph(self, profile: RuntimeProfile, *, start_components: bool = True) -> _ActiveGraph:
        host_services = self._build_host_registry()
        resolved = self.module_catalog.resolve(profile, host_services)
        registry = host_services.clone()
        graph = _ActiveGraph(profile, registry)
        started: list[tuple[Any, Any, ModuleContribution]] = []
        newly_applied: set[tuple[str, int]] = set()
        try:
            for descriptor in resolved.descriptors:
                config = profile.modules[descriptor.id]
                context = ModuleContext(self, descriptor, config, registry, profile)
                for migration in descriptor.migrations():
                    key = (descriptor.id, migration.revision)
                    if key not in self._applied_module_migrations:
                        await migration.apply(context)
                        newly_applied.add(key)
                instance = await descriptor.prepare(context)
                contribution = await instance.start()
                if not isinstance(contribution, ModuleContribution):
                    raise TypeError(f"Module {descriptor.id} returned an invalid contribution")
                for binding in contribution.services:
                    registry.bind(binding.key, binding.value, owner=descriptor.id)
                for provider in contribution.search_providers:
                    registry.bind_many("search.providers", provider, owner=descriptor.id)
                for capability in contribution.capabilities:
                    self._register_module_capability(capability, profile.generation)
                started.append((descriptor, instance, contribution))
            graph.instances = started
            graph.prompt_blocks = tuple(block for _descriptor, _instance, contribution in started
                                        for block in contribution.prompt_blocks)
            graph.routes = tuple(route for _descriptor, _instance, contribution in started
                                 for route in contribution.routes)
            graph.health_checks = tuple(check for _descriptor, _instance, contribution in started
                                        for check in contribution.health_checks)
            graph.search_providers = registry.all("search.providers")
            graph.workers = tuple(worker for _descriptor, _instance, contribution in started
                                  for worker in contribution.workers)
            graph.event_ingresses = tuple(ingress for _descriptor, _instance, contribution in started
                                          for ingress in contribution.event_ingresses)
            graph.indices = tuple(index for _descriptor, _instance, contribution in started
                                  for index in contribution.indices)
            if start_components:
                await self._start_graph_components(graph)
            graph.drained.set()
            self._applied_module_migrations.update(newly_applied)
            return graph
        except BaseException:
            self.capabilities.revoke_generation(profile.generation)
            for _descriptor, instance, _contribution in reversed(started):
                try:
                    await instance.close()
                except Exception:
                    pass
            raise

    async def _start_graph_components(self, graph: _ActiveGraph) -> None:
        if graph.components_started:
            return
        started: list[Any] = []
        try:
            for component in (*graph.indices, *graph.event_ingresses, *graph.workers):
                identifier = id(component)
                async with self._component_lock:
                    references = self._component_references.get(identifier, 0)
                    if references:
                        self._component_references[identifier] = references + 1
                        started.append(component)
                        continue
                    start = getattr(component, "start", None)
                    if callable(start):
                        required = [parameter for parameter in inspect.signature(start).parameters.values()
                                    if parameter.default is inspect.Parameter.empty
                                    and parameter.kind in {inspect.Parameter.POSITIONAL_ONLY,
                                                           inspect.Parameter.POSITIONAL_OR_KEYWORD}]
                        value = start(self) if required else start()
                        if asyncio.iscoroutine(value):
                            await value
                    self._component_references[identifier] = 1
                    started.append(component)
            graph.components_started = True
        except BaseException:
            for component in reversed(started):
                await self._release_component(component)
            raise

    async def _release_component(self, component: Any) -> None:
        identifier = id(component)
        async with self._component_lock:
            references = self._component_references.get(identifier, 0)
            if references > 1:
                self._component_references[identifier] = references - 1
                return
            self._component_references.pop(identifier, None)
            close = getattr(component, "close", None)
            if callable(close):
                value = close()
                if asyncio.iscoroutine(value):
                    await value

    def _register_module_capability(self, capability: CapabilityContribution, generation: int) -> None:
        register = (self.capabilities.register if capability.managed
                    else self.capabilities.register_user_source)
        kwargs = {"target_label": capability.target_label, "graph_generation": generation}
        if capability.managed:
            kwargs["trusted_effects"] = capability.trusted_effects
        register(capability.source_id, capability.source, capability.executor, **kwargs)

    async def _close_graph(self, graph: _ActiveGraph) -> list[Exception]:
        errors: list[Exception] = []
        if graph.components_started:
            for component in reversed((*graph.indices, *graph.event_ingresses, *graph.workers)):
                try:
                    await self._release_component(component)
                except Exception as exc:
                    errors.append(exc)
            graph.components_started = False
        for _descriptor, instance, _contribution in reversed(graph.instances):
            try:
                with runtime_scope(self):
                    await instance.close()
            except Exception as exc:
                errors.append(exc)
        self.capabilities.revoke_generation(graph.profile.generation)
        self._retired_graphs.pop(graph.profile.generation, None)
        return errors

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
                with runtime_scope(self):
                    self._active_graph = await self._prepare_graph(
                        self._initial_profile, start_components=False
                    )
                self.capabilities.activate_generation(self._active_graph.profile.generation)
                for module in self.modules:
                    # A partially started module owns rollback of its own
                    # resources; successfully started modules are closed here.
                    with runtime_scope(self):
                        await module.start(self)
                    self._started_modules.append(module)
                await self._start_graph_components(self._active_graph)
            except BaseException:
                if self._active_graph is not None:
                    await self._close_graph(self._active_graph)
                    self._active_graph = None
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
            graphs = ([self._active_graph] if self._active_graph is not None else []) + list(self._retired_graphs.values())
            seen_graphs = set()
            for graph in graphs:
                if id(graph) in seen_graphs:
                    continue
                seen_graphs.add(id(graph))
                errors.extend(await self._close_graph(graph))
            self._active_graph = None
            self._retired_graphs.clear()
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

    async def reconfigure(self, profile: RuntimeProfile, *, expected_generation: int,
                          mode: str = "drain") -> ReconfigureReceipt:
        """Atomically replace the host-selected graph with installed modules.

        Preparation and migrations happen before the swap.  Newly registered
        capability sources are tagged with the candidate generation and are
        therefore invisible to runs admitted on the old graph.
        """
        if mode not in {"drain", "force"}:
            raise ValueError("Reconfiguration mode must be 'drain' or 'force'")
        async with self._lifecycle:
            self._admission()
            current = self._active_graph
            if current is None or current.profile.generation != expected_generation:
                raise ModuleReconfigurationError("Runtime profile generation changed")
            if profile.generation <= expected_generation:
                raise ModuleReconfigurationError("A new profile must advance the generation")
            current_profile = current.profile
            # Resolution and descriptor validation are pure and happen before
            # migrations, instance preparation or capability registration.
            self.module_catalog.resolve(profile, self._build_host_registry())
        await self._preflight_reconfiguration(current_profile, profile)
        with runtime_scope(self):
            candidate = await self._prepare_graph(profile)
        async with self._lifecycle:
            current = self._active_graph
            if current is None or current.profile.generation != expected_generation:
                await self._close_graph(candidate)
                raise ModuleReconfigurationError("Runtime profile changed while preparing the candidate")
            old = current
            old.draining = True
            if old.leases == 0:
                old.drained.set()
            self._retired_graphs[old.profile.generation] = old
            self._active_graph = candidate
            self.capabilities.activate_generation(profile.generation)
        for _descriptor, instance, _contribution in old.instances:
            await instance.drain()
        for component in reversed((*old.indices, *old.event_ingresses, *old.workers)):
            drain = getattr(component, "drain", None)
            if callable(drain):
                await drain()
        if mode == "force":
            tasks = [task for run_id, task in tuple(self._tasks.items())
                     if self._run_graphs.get(run_id) is old and not task.done()]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        else:
            await old.drained.wait()
        errors = await self._close_graph(old)
        if errors:
            raise ExceptionGroup("Retired module graph shutdown failed", errors)
        old_ids = old.profile.module_ids
        new_ids = profile.module_ids
        return ReconfigureReceipt(
            expected_generation,
            profile.generation,
            tuple(ModuleStatus(descriptor.id, descriptor.version,
                               tuple(sorted(profile.modules[descriptor.id].surfaces)))
                  for descriptor, _instance, _contribution in candidate.instances),
            tuple(sorted(old_ids - new_ids)),
            mode,
        )

    async def _preflight_reconfiguration(
        self, current: RuntimeProfile, candidate: RuntimeProfile
    ) -> None:
        removed = current.module_ids - candidate.module_ids
        if not removed:
            return
        inspectors = self.services.registry.all("module.reference_inspector")
        graph = self._active_graph
        if graph is not None:
            inspectors = (*inspectors, *graph.services.all("module.reference_inspector"))
        merged: dict[str, list[str]] = {}
        seen: set[int] = set()
        for inspector in inspectors:
            if id(inspector) in seen:
                continue
            seen.add(id(inspector))
            result = await inspector.references_for_removed_modules(
                frozenset(removed), current_profile=current,
                candidate_profile=candidate,
            )
            for module_id, references in result.items():
                merged.setdefault(str(module_id), []).extend(str(ref) for ref in references)
        blocked = {
            module_id: tuple(dict.fromkeys(references))
            for module_id, references in merged.items() if references
        }
        if blocked:
            raise ModuleReferenceConflict(blocked)

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
            delegations = self.service(DelegationService)
            if delegations is None or not await delegations.validate(context):
                raise PermissionError('Execution delegation is missing or revoked')
        await require_authorized(self.services.authorizer,context,action,resource,audience=audience)

    @asynccontextmanager
    async def _credentials(self, context: ExecutionContext):
        """Optional host scope resolves ephemeral credentials from a delegation.

        The host owns refresh and teardown. Neither credentials nor their
        resolver are serialized into the execution context or run record.
        """
        delegations = self.service(DelegationService)
        scope = getattr(delegations, 'execution_scope', None)
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
        from .persistence import complete_before_cancelling
        async with self._lifecycle:
            # Closing cannot pass an acceptance whose execution has not yet
            # been registered. Disconnecting the submitter cannot strand it.
            return await complete_before_cancelling(self._admit_and_start(request, context, executor))

    async def _admit_and_start(self, request: RunRequest, context: ExecutionContext, executor: AgentExecutor) -> RunRecord:
        self._admission()
        graph = self._active_graph
        if graph is None:
            raise RuntimeError("Runtime module graph is unavailable")
        request = copy.deepcopy(request)
        if request.session_id != context.session_id:
            raise ValueError('Request and verified context must identify the same session')
        async with self._credentials(context):
            await self._authorize(context,'run.submit',request.session_id,context.tenant_id)
            if request.steer_run_id is not None:
                await self._steering_target(request,context)
        record, created = await self.services.store.accept(request,context)
        if created:
            graph.acquire()
            capability_revision = self.capabilities.snapshot_revision()
            self._run_graphs[record.run_id] = graph
            self._run_capability_revisions[record.run_id] = capability_revision
            try:
                await self.services.store.append_event(record.run_id, "run.profile", {
                    "generation": graph.profile.generation,
                    "modules": [
                        {"id": descriptor.id, "version": descriptor.version,
                         "surfaces": sorted(graph.profile.modules[descriptor.id].surfaces)}
                        for descriptor, _instance, _contribution in graph.instances
                    ],
                    "capability_revision": capability_revision,
                })
                self._tasks[record.run_id] = asyncio.create_task(
                    self._execute(request,context,executor,graph,capability_revision),
                    name='openagent-run-'+record.run_id,
                )
            except BaseException:
                self._run_graphs.pop(record.run_id, None)
                self._run_capability_revisions.pop(record.run_id, None)
                graph.release()
                raise
        return record

    async def _steering_target(self, request: RunRequest, context: ExecutionContext) -> RunRecord:
        target = await self.get_run(request.steer_run_id,context)
        if target.session_id != request.session_id or target.tenant_id != context.tenant_id:
            raise PermissionError('Steering target belongs to a different session')
        await self._authorize(context,'run.cancel',target.session_id,target.tenant_id)
        return target

    async def _execute(self, request: RunRequest, context: ExecutionContext, executor: AgentExecutor,
                       graph: _ActiveGraph, capability_revision: int) -> None:
        try:
            with execution_scope(self,context,request.run_id,
                                 module_generation=graph.profile.generation,
                                 capability_revision=capability_revision):
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
            self._run_graphs.pop(request.run_id,None)
            self._run_capability_revisions.pop(request.run_id,None)
            graph.release()

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

    async def accepted_request(self, run_id: str, context: ExecutionContext):
        """Read original admission facts to reconcile a reconnect or retry.

        This is an observation with fresh authorization. It never rebinds the
        original authority/capabilities to a new device or restarts execution.
        """
        record=await self.get_run(run_id,context)
        await self._authorize(context,'run.replay',record.session_id,record.tenant_id)
        return await self.services.store.accepted_request(run_id)

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
