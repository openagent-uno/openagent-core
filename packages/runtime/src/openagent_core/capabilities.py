"""One catalog and dispatcher for functions, MCPs and connected applications."""
from __future__ import annotations
from dataclasses import dataclass, field, replace, asdict, is_dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol
import copy
import secrets
import json
from .contracts import Authorizer, CapabilityLease, ExecutionContext, ResourceRef, require_authorized


class CapabilityUnavailable(LookupError):
    pass


@dataclass(frozen=True, slots=True)
class ToolDefinition:
    name: str
    description: str
    input_schema: Mapping[str, Any]
    effects: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class ToolDescriptor:
    tool_ref: str
    name: str
    description: str
    input_schema: Mapping[str, Any]
    source_id: str
    target_label: str
    effects: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class ToolInventoryEntry:
    """Read-only durable binding metadata; never an invocation reference."""
    source_id: str
    name: str
    description: str
    input_schema: Mapping[str, Any]
    target_label: str


class CapabilitySource(Protocol):
    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]: ...


class ToolExecutor(Protocol):
    async def call_tool(self, name: str, arguments: Mapping[str, Any], context: ExecutionContext) -> Any: ...


@dataclass(slots=True)
class _Registration:
    source_id: str
    source: CapabilitySource
    executor: ToolExecutor
    target_label: str
    managed: bool
    lease: CapabilityLease | None
    trusted_effects: Mapping[str, frozenset[str]] | frozenset[str] = frozenset()
    graph_generation: int | None = None
    visible_from_revision: int = 0
    revoked: bool = False
    references: dict[str, tuple[str, ToolDefinition]] = field(default_factory=dict)


EffectObserver = Callable[[ToolDescriptor, Mapping[str, Any], Any, ExecutionContext, str], Awaitable[None]]


class CapabilityCatalog:
    """Per-runtime trusted registrations. A persisted reference grants nothing.

    Registration is a host API. User management must call install/remove; no
    mutable DB metadata can turn a host registration into a user-owned source.
    """
    def __init__(self, authorizer: Authorizer, *, allow_dynamic: bool = False,
                 observers: tuple[EffectObserver, ...] = ()) -> None:
        self.authorizer = authorizer
        self.allow_dynamic = allow_dynamic
        self.observers = observers
        self._sources: dict[tuple[str, int | None], _Registration] = {}
        self._refs: dict[str, tuple[_Registration, ToolDefinition]] = {}
        self._revision = 0
        self._active_generation: int | None = None

    @property
    def revision(self) -> int:
        return self._revision

    def snapshot_revision(self) -> int:
        """Capture the topology visible to a newly admitted run."""
        return self._revision

    def activate_generation(self, generation: int | None) -> None:
        self._active_generation = generation

    def _next_revision(self) -> int:
        self._revision += 1
        return self._revision

    def register(self, source_id: str, source: CapabilitySource, executor: ToolExecutor, *,
                 target_label: str, lease: CapabilityLease | None = None,
                 trusted_effects: Mapping[str, frozenset[str]] | frozenset[str] = frozenset(),
                 graph_generation: int | None = None) -> None:
        self._register(source_id, source, executor, target_label=target_label, managed=True,
                       lease=lease, trusted_effects=trusted_effects,
                       graph_generation=graph_generation)

    def register_user_source(self, source_id: str, source: CapabilitySource, executor: ToolExecutor, *,
                             target_label: str, graph_generation: int | None = None) -> None:
        """Restore a user-owned source from the host's trusted registry service.

        This is a composition API, never a REST deserializer. Imported MCP
        metadata cannot choose this path or promote itself to a managed source.
        """
        if not self.allow_dynamic:
            raise PermissionError("This host has a fixed capability catalog")
        self._register(source_id, source, executor, target_label=target_label, managed=False,
                       graph_generation=graph_generation)

    def _register(self, source_id: str, source: CapabilitySource, executor: ToolExecutor, *,
                  target_label: str, managed: bool, lease: CapabilityLease | None = None,
                  trusted_effects: Mapping[str, frozenset[str]] | frozenset[str] = frozenset(),
                  graph_generation: int | None = None) -> None:
        key = (source_id, graph_generation)
        if not source_id or key in self._sources:
            raise ValueError("Capability source IDs must be unique")
        if lease and lease.source_id != source_id:
            raise ValueError("The lease must identify this exact source")
        registration = _Registration(source_id, source, executor, target_label, managed,
                                     lease, trusted_effects, graph_generation,
                                     self._next_revision())
        self._sources[key] = registration

    def revoke(self, source_id: str) -> None:
        registrations = [registration for (name, _generation), registration in self._sources.items()
                         if name == source_id]
        if registrations:
            self._next_revision()
        for registration in registrations:
            registration.revoked = True
            self._sources.pop((registration.source_id, registration.graph_generation), None)
            for reference, _ in registration.references.values():
                self._refs.pop(reference, None)

    def revoke_registration(self, source_id: str, graph_generation: int | None) -> bool:
        registration = self._sources.pop((source_id, graph_generation), None)
        if registration is None:
            return False
        registration.revoked = True
        for reference, _ in registration.references.values():
            self._refs.pop(reference, None)
        self._next_revision()
        return True

    def revoke_generation(self, generation: int) -> None:
        registrations = [registration for (_name, current), registration in self._sources.items()
                         if current == generation]
        if registrations:
            self._next_revision()
        for registration in registrations:
            registration.revoked = True
            self._sources.pop((registration.source_id, registration.graph_generation), None)
            for reference, _ in registration.references.values():
                self._refs.pop(reference, None)

    def revoke_lease(self, lease: CapabilityLease) -> bool:
        """A delayed disconnect must never revoke a newer connection."""
        matches = [registration for (source_id, _generation), registration in self._sources.items()
                   if source_id == lease.source_id and registration.lease == lease]
        if len(matches) != 1:
            return False
        registration = matches[0]
        registration.revoked = True
        self._sources.pop((registration.source_id, registration.graph_generation), None)
        for reference, _ in registration.references.values():
            self._refs.pop(reference, None)
        self._next_revision()
        return True

    async def install(self, source_id: str, source: CapabilitySource, executor: ToolExecutor,
                      context: ExecutionContext, *, target_label: str) -> None:
        if not self.allow_dynamic:
            raise PermissionError("This host has a fixed capability catalog")
        await require_authorized(self.authorizer, context, "catalog.install", ResourceRef("capability", context.tenant_id, source_id))
        self._register(source_id, source, executor, target_label=target_label, managed=False)

    async def remove(self, source_id: str, context: ExecutionContext) -> None:
        registration = next((registration for registration in self._registrations_for_context(context)
                             if registration.source_id == source_id), None)
        if registration is None:
            raise CapabilityUnavailable(source_id)
        if registration.managed or not self.allow_dynamic:
            raise PermissionError("This capability source is managed by the host")
        await require_authorized(self.authorizer, context, "catalog.remove", ResourceRef("capability", context.tenant_id, source_id))
        self.revoke(source_id)

    def _execution_snapshot(self) -> tuple[int | None, int]:
        try:
            from .runtime import current_capability_revision, current_module_generation, current_runtime
            runtime = current_runtime()
            if runtime is not None and runtime.capabilities is self:
                generation = current_module_generation()
                revision = current_capability_revision()
                return (self._active_generation if generation is None else generation,
                        self._revision if revision is None else revision)
        except ImportError:
            pass
        return self._active_generation, self._revision

    def _available(self, registration: _Registration, context: ExecutionContext) -> bool:
        generation, revision = self._execution_snapshot()
        return (not registration.revoked and
                self._sources.get((registration.source_id, registration.graph_generation)) is registration and
                registration.visible_from_revision <= revision and
                (registration.graph_generation is None or registration.graph_generation == generation) and
                (registration.lease is None or (
                    not getattr(context, "deferred", False) and
                    registration.lease in getattr(context, "capabilities", ())
                )))

    def _registrations_for_context(self, context: ExecutionContext) -> tuple[_Registration, ...]:
        return tuple(registration for registration in self._sources.values()
                     if self._available(registration, context))

    def has_source(self, source_id: str, context: ExecutionContext) -> bool:
        """Return whether *source_id* belongs to the current run snapshot.

        Hosts use this read-only check before applying their own audience and
        delegation policy.  Registration is the trusted composition boundary:
        callers cannot make an arbitrary source valid by naming it, and the
        result observes generation, admission revision, lease and revocation.
        """
        return any(registration.source_id == source_id
                   for registration in self._registrations_for_context(context))

    async def inspect(self, context: Any, *, authorizer: Authorizer) -> tuple[ToolInventoryEntry, ...]:
        """Inspect host-owned schema metadata without constructing an agent turn.

        A host supplies its management authorizer. Only sources explicitly
        implementing inspect participate; temporary client registrations never
        do. Discovery/invocation still require the regular ExecutionContext.
        """
        result = []
        for registration in self._registrations_for_context(context):
            inspect = getattr(registration.source, "inspect", None)
            if registration.lease is not None or not callable(inspect):
                continue
            resource = ResourceRef("capability", context.tenant_id, registration.source_id)
            if not await authorizer.authorize(context, "catalog.inspect", resource, audience=()):
                continue
            definitions = tuple(await inspect(context))
            if (not self._available(registration, context) or
                    not await authorizer.authorize(context, "catalog.inspect", resource, audience=())):
                continue
            names = set()
            for definition in definitions:
                if definition.name in names or len(result) >= 4096:
                    raise ValueError("Catalog inventory is ambiguous or exceeds its bound")
                names.add(definition.name)
                result.append(ToolInventoryEntry(registration.source_id, definition.name, definition.description,
                    copy.deepcopy(definition.input_schema), registration.target_label))
        return tuple(result)

    async def discover(self, context: ExecutionContext) -> tuple[ToolDescriptor, ...]:
        result: list[ToolDescriptor] = []
        for registration in self._registrations_for_context(context):
            resource = ResourceRef("capability", context.tenant_id, registration.source_id)
            if not await self.authorizer.authorize(context, "tool.discover", resource, audience=context.audience):
                continue
            definitions = await registration.source.discover(context)
            if not self._available(registration, context):
                continue
            seen: set[str] = set()
            for definition in definitions:
                definition = replace(definition, effects=definition.effects & (registration.trusted_effects.get(definition.name, frozenset()) if isinstance(registration.trusted_effects, Mapping) else registration.trusted_effects))
                if definition.name in seen:
                    raise ValueError("A source returned ambiguous duplicate tool names")
                seen.add(definition.name)
                previous = registration.references.get(definition.name)
                if previous is None or previous[1] != definition:
                    if previous:
                        self._refs.pop(previous[0], None)
                    reference = secrets.token_urlsafe(32)
                    definition = copy.deepcopy(definition)
                    registration.references[definition.name] = reference, definition
                    self._refs[reference] = registration, definition
                else:
                    reference = previous[0]
                result.append(self._descriptor(reference, registration, definition))
        return tuple(result)

    @staticmethod
    def _descriptor(reference: str, registration: _Registration, definition: ToolDefinition) -> ToolDescriptor:
        return ToolDescriptor(reference, definition.name, definition.description, copy.deepcopy(definition.input_schema),
                              registration.source_id, registration.target_label, definition.effects)

    async def call_tool(self, tool_ref: str, arguments: Mapping[str, Any], context: ExecutionContext, *, call_id: str | None = None) -> Any:
        call_id = call_id or secrets.token_urlsafe(24)
        binding = self._refs.get(tool_ref)
        if binding is None:
            raise CapabilityUnavailable("Unknown or revoked tool reference; discover current tools")
        registration, definition = binding
        if not self._available(registration, context):
            raise CapabilityUnavailable("The exact capability instance is unavailable")
        resource = ResourceRef("tool", context.tenant_id, registration.source_id + "/" + definition.name)
        from .runtime import current_runtime, current_run_id
        runtime = current_runtime()
        run_id = current_run_id()
        durable = runtime is not None and runtime.capabilities is self and run_id is not None
        async def authorize(action):
            if durable:
                await runtime.authorize(context,action,resource,audience=context.audience)
            else:
                await require_authorized(self.authorizer,context,action,resource,audience=context.audience)
        await authorize("tool.call")
        current = tuple(replace(d, effects=d.effects & (registration.trusted_effects.get(d.name, frozenset()) if isinstance(registration.trusted_effects, Mapping) else registration.trusted_effects))
                        for d in await registration.source.discover(context))
        if definition not in current or not self._available(registration, context):
            raise CapabilityUnavailable("Tool was removed or changed after discovery")
        descriptor = self._descriptor(tool_ref, registration, definition)
        if durable:
            host = {"kind":"capability","device_label":registration.target_label,"source_id":registration.source_id}
            if registration.lease is not None:
                host.update(instance_id=registration.lease.instance_id,generation=registration.lease.generation)
            await runtime.services.store.begin_tool(run_id,call_id,
                {"tool_ref":tool_ref,"source_id":registration.source_id,"name":definition.name,
                 "target_label":registration.target_label,"effects":sorted(definition.effects),"execution_host":host},arguments)
        try:
            result = await registration.executor.call_tool(definition.name, copy.deepcopy(dict(arguments)), context)
            # Publication is distinct from execution. Do not persist the
            # private result into shared history when this authorization fails.
            await authorize("tool.publish")
            if durable:
                if hasattr(result,"model_dump"):
                    envelope = result.model_dump(mode="json",by_alias=True)
                elif is_dataclass(result):
                    envelope = asdict(result)
                else:
                    envelope = result
                # Reject an unserializable result without inventing a lossy
                # string representation of attachments or structured content.
                envelope = json.loads(json.dumps(envelope,allow_nan=False))
        except BaseException as exc:
            if durable:
                await runtime.services.store.finish_tool(run_id,call_id,
                    error={"error_type":type(exc).__name__,"effects":"unknown","retry_allowed":False})
            raise
        if durable:
            await runtime.services.store.finish_tool(run_id,call_id,result=envelope)
        for observer in self.observers:
            await observer(descriptor, arguments, result, context, call_id)
        return result


class FunctionSource:
    def __init__(self, tools: tuple[ToolDefinition, ...], functions: Mapping[str, Callable[..., Awaitable[Any]]]) -> None:
        self.tools = tools
        self.functions = dict(functions)
        if len({t.name for t in tools}) != len(tools) or {t.name for t in tools} != set(functions):
            raise ValueError("Every declared tool must have exactly one implementation")

    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        return self.tools

    async def call_tool(self, name: str, arguments: Mapping[str, Any], context: ExecutionContext) -> Any:
        return await self.functions[name](dict(arguments), context)
