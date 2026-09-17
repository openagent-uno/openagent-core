"""Supported adapter for the existing agent algorithm and module dispatcher.

Importing Agent requests the optional engine dependencies. Hosts construct the
agent with explicitly selected model, storage and pool; Runtime owns admission,
authorization, cancellation and durable execution identity.
"""
from __future__ import annotations
from typing import Any
from .contracts import ExecutionContext, RunRequest, ResourceRef, ModelCatalog, require_authorized
from .runtime import Runtime, current_execution_context, current_runtime


def __getattr__(name: str):
    if name == 'Agent':
        from .core.agent import Agent
        return Agent
    if name == 'MemoryDB':
        from .memory.db import MemoryDB
        return MemoryDB
    if name == "ModelDispatcher":
        from .models.dispatcher import ModelDispatcher
        return ModelDispatcher
    if name == "NativeProvider":
        from .models.native_provider import NativeProvider
        return NativeProvider
    if name == 'MCPPool':
        from .mcp.pool import MCPPool
        return MCPPool
    if name == 'PoolCapabilitySource':
        from .mcp.catalog import PoolCapabilitySource
        return PoolCapabilitySource
    raise AttributeError(name)


class AgentExecutor:
    def __init__(self, agent: Any, *, owns_agent: bool = True) -> None:
        self.agent = agent
        self.owns_agent = owns_agent

    async def start(self, runtime: Runtime) -> None:
        from .core.hooks import set_hooks, set_quick_commands
        set_hooks(self.agent.config.get("hooks", {}))
        set_quick_commands(self.agent.config.get("quick_commands", {}))
        try:
            await self.agent.initialize()
            bind = getattr(self.agent.capability_pool, "bind_capability_catalog", None)
            if callable(bind):
                bind(runtime.capabilities, trusted_modules=runtime.enabled_module_ids,
                     user_sources=runtime.service("user_sources", frozenset()))
        except BaseException:
            if self.owns_agent:
                await self.agent.shutdown()
            raise

    async def close(self) -> None:
        if self.owns_agent:
            await self.agent.shutdown()

    async def execute(self, request: RunRequest, context: ExecutionContext, runtime: Runtime) -> str:
        from .core.agent import clear_run_failure, take_run_failure
        clear_run_failure()
        model = None
        if request.model_ref is not None:
            models = runtime.service(ModelCatalog)
            if models is None:
                raise ValueError("A model override requires a host-supplied ModelCatalog adapter")
            model = await models.resolve(request.model_ref, context)
        chunks = []
        async def can_publish() -> None:
            await require_authorized(runtime.services.authorizer, context, "run.publish",
                ResourceRef("session",context.tenant_id,context.session_id),audience=context.audience)
        async def status(text: str) -> None:
            await can_publish()
            await runtime.services.store.append_event(request.run_id,"run.status",{"text":text})
        final_text = None
        async for event in self.agent.run_stream(request.input,session_id=request.session_id,
                attachments=list(request.attachments),on_status=status,model_override=model,
                author={"kind":context.author.kind,"handle":context.author.key}):
            await can_publish()
            kind = event.get("kind")
            if kind == "delta":
                chunks.append(event.get("text") or "")
            elif kind == "done":
                final_text = event.get("text") or ""
            elif kind == "error":
                raise RuntimeError("Agent stream failed")
            await runtime.services.store.append_event(request.run_id,"run.stream",event)
        result = final_text if final_text is not None else "".join(chunks)
        failure = take_run_failure()
        if failure:
            raise RuntimeError('Agent execution failed')
        return result


async def call_tool(pool_or_catalog: Any, source_ref: str, tool_name: str, arguments: dict | None = None) -> Any:
    """Resolve a durable logical tool binding through the current authorized catalog.

    Used by workflow and product services, never a privileged pool bypass.
    Interactive callers use the opaque reference returned by discovery.
    """
    context=current_execution_context()
    runtime=current_runtime()
    if context is None or runtime is None:
        raise PermissionError('Tool invocation requires an authenticated runtime execution')
    catalog=runtime.capabilities
    engine_agent=getattr(runtime.services.executor,"agent",None)
    current_pool=getattr(engine_agent,"capability_pool",None)
    if pool_or_catalog is not catalog and pool_or_catalog is not current_pool:
        raise PermissionError("The tool source belongs to another runtime")
    matches=[tool for tool in await catalog.discover(context) if tool.source_id==source_ref and tool.name==tool_name]
    if len(matches)!=1:
        raise LookupError('The exact durable tool destination is unavailable or ambiguous')
    return await catalog.call_tool(matches[0].tool_ref,arguments or {},context)


class AgentModelCatalog:
    """Public model service over the host's existing configured engine catalog."""
    def __init__(self, agent: Any, authorizer: Any) -> None:
        self.agent=agent
        self.authorizer=authorizer

    async def list_models(self, context: ExecutionContext):
        await require_authorized(self.authorizer,context,"model.list",ResourceRef("agent",context.tenant_id,context.agent_id))
        if self.agent.memory_db is None:
            return ()
        return tuple(await self.agent.memory_db.list_models())

    async def resolve(self, model_ref: str, context: ExecutionContext):
        await require_authorized(self.authorizer,context,"model.use",ResourceRef("model",context.tenant_id,model_ref))
        dispatcher=self.agent.model
        resolve=getattr(dispatcher,"build_override_model",None)
        if not callable(resolve):
            raise LookupError("The configured model catalog cannot resolve this pin")
        model=resolve(model_ref)
        if model is None:
            raise LookupError("Pinned model is unavailable")
        return model
