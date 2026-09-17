from __future__ import annotations
from dataclasses import asdict, dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Protocol

from aiohttp import web
from openagent_core import (Runtime, RuntimeServices, RuntimeSettings, ModuleCatalog,
    ModuleConfig, RuntimeProfile)
from openagent_core.capabilities import CapabilityCatalog, FunctionSource, ToolDefinition
from openagent_core.contracts import ExecutionContext, IdempotencyConflict, PrincipalRef, RunRequest
from openagent_core.engine import Agent, AgentExecutor, MemoryDB, NativeProvider, MCPPool
from openagent_module_sessions import descriptor as sessions_descriptor
from openagent_module_tool_discovery import descriptor as tool_discovery_descriptor
from openagent_storage_sqlite import SqliteRuntimeStore


class IdentityAdapter(Protocol):
    """Implemented by Replio authentication, never by the agent core."""
    async def authenticate(self, bearer: str) -> PrincipalRef: ...
    async def active(self, principal: PrincipalRef) -> bool: ...


@dataclass(frozen=True)
class Document:
    id: str
    title: str
    text: str


class ReplioPolicy:
    def __init__(self, *, identity, tenant_id, project_id, agent_id, sessions):
        self.identity=identity
        self.tenant_id, self.project_id, self.agent_id=tenant_id, project_id, agent_id
        self.sessions=MappingProxyType({name: tuple(members) for name,members in sessions.items()})

    async def context(self, principal, session_id):
        if principal.authority!='replio' or principal.tenant_id!=self.tenant_id or not await self.identity.active(principal):
            raise PermissionError('Replio identity is not authorized for this project')
        members=self.sessions.get(session_id,())
        if principal not in members:
            raise PermissionError('Replio session membership is required')
        return ExecutionContext(principal,principal,principal,session_id,self.agent_id,members,
                                scopes=('project:'+self.project_id,))

    async def authorize(self,context,action,resource,*,audience=()):
        members=self.sessions.get(context.session_id,())
        if (context.agent_id!=self.agent_id or context.tenant_id!=self.tenant_id
            or resource.tenant_id!=self.tenant_id or context.authority!=context.initiator
            or context.author!=context.initiator or context.initiator not in members
            or context.initiator.authority!='replio' or context.deferred or context.capabilities
            or not await self.identity.active(context.initiator)):
            return False
        for recipient in audience:
            if recipient not in members or not await self.identity.active(recipient):
                return False
        if action.startswith('run.'):
            return (resource.kind=='session' and resource.resource_id==context.session_id
                and action in {'run.submit','run.execute','run.read','run.replay','run.publish','run.cancel'})
        if action in {'tool.discover','tool.call','tool.publish'}:
            return resource.resource_id in {'replio-project','replio-project/list_documents','replio-project/read_document'}
        return False


class ReplioHost:
    """One project runtime with Replio-owned session membership and documents.

    The caller owns identity, project provisioning and provider configuration.
    Creating this object does not open listeners or create an OpenAgent user.
    """
    def __init__(self, *, data_dir: Path, tenant_id: str, project_id: str,
                 identity: IdentityAdapter, sessions: Mapping[str,tuple[PrincipalRef,...]],
                 documents: tuple[Document,...], provider_model: str, provider_base_url: str,
                 provider_key: str, system_prompt: str='Answer using this Replio project and cite document titles.'):
        if not tenant_id or not project_id:
            raise ValueError('The host must select a tenant and project')
        self.identity=identity
        self.policy=ReplioPolicy(identity=identity,tenant_id=tenant_id,project_id=project_id,
            agent_id='replio:'+project_id,sessions=sessions)
        for members in self.policy.sessions.values():
            if not members or any(p.authority!='replio' or p.tenant_id!=tenant_id or p.kind!='user' for p in members):
                raise ValueError('Session members must belong to the selected Replio tenant')
        self.documents=MappingProxyType({d.id:d for d in documents})
        if len(self.documents)!=len(documents):
            raise ValueError('Document IDs must be unique within the project')
        self.tool_audit=[]
        self.catalog=CapabilityCatalog(self.policy,allow_dynamic=False)
        source=FunctionSource((
            ToolDefinition('list_documents','List documents in the current Replio project.',
                {'type':'object','properties':{},'additionalProperties':False}),
            ToolDefinition('read_document','Read one document from the current Replio project.',
                {'type':'object','properties':{'document_id':{'type':'string'}},
                 'required':['document_id'],'additionalProperties':False}),
        ),{'list_documents':self.list_documents,'read_document':self.read_document})
        self.catalog.register('replio-project',source,source,target_label='Replio project '+project_id)
        data_dir=Path(data_dir).absolute()
        db_path=data_dir/'runtime.sqlite3'
        memory=MemoryDB(str(db_path))
        model=NativeProvider('replio:'+provider_model,api_key=provider_key,db_path=str(db_path),
            providers_config=[{'name':'replio','enabled':True,'framework':'api-based','kind':'llm','base_url':provider_base_url}])
        pool=MCPPool.from_config([{'builtin':'tool-search'}],include_defaults=False)
        agent=Agent(name=self.policy.agent_id,model=model,memory=memory,mcp_pool=pool,
            system_prompt=system_prompt,config={'skills':{'enabled':False},'ptc':{'enabled':False},
                'memory':{'db_path':str(db_path),'vault_path':str(data_dir/'unused-vault')}})
        self.executor=AgentExecutor(agent)
        profile=RuntimeProfile(1,{
            'tool-discovery':ModuleConfig(frozenset({'service','agent_tools'})),
            # Replio exposes session administration through its own product UI;
            # the module service is present without adding agent-visible tools.
            'sessions':ModuleConfig(frozenset({'service','host_api'})),
        })
        self.runtime=Runtime(RuntimeSettings(self.policy.agent_id,data_dir,()),
            RuntimeServices(SqliteRuntimeStore(db_path),self.executor,self.policy,self.catalog),
            modules=(self.executor,),profile=profile,
            module_catalog=ModuleCatalog((tool_discovery_descriptor,sessions_descriptor)))

    async def list_documents(self,args,context):
        if args: raise ValueError('list_documents takes no arguments')
        self.tool_audit.append(('list_documents',context.initiator))
        return {'documents':[{'id':d.id,'title':d.title} for d in self.documents.values()]}

    async def read_document(self,args,context):
        if set(args)!={'document_id'} or not isinstance(args['document_id'],str):
            raise ValueError('read_document requires only document_id')
        document=self.documents.get(args['document_id'])
        if document is None: raise LookupError('No such document in this Replio project')
        self.tool_audit.append(('read_document',context.initiator))
        return {'content':[{'type':'text','text':document.text}], 'structuredContent':asdict(document),
            '_meta':{'project_id':self.policy.project_id,'requested_by':context.initiator.subject_id}}


def create_app(host: ReplioHost):
    """Optional HTTP adapter; every route delegates to the same runtime policy."""
    @web.middleware
    async def authenticated(request,handler):
        try:
            header=request.headers.get('Authorization','')
            if not header.startswith('Bearer '): raise PermissionError('Bearer identity required')
            principal=await host.identity.authenticate(header[7:])
            request['context']=await host.policy.context(principal,request.match_info.get('session_id',''))
            return await handler(request)
        except PermissionError as exc: raise web.HTTPForbidden(text=str(exc)) from exc
        except LookupError as exc: raise web.HTTPNotFound(text='Unknown resource') from exc
        except IdempotencyConflict as exc: raise web.HTTPConflict(text=str(exc)) from exc
        except ValueError as exc: raise web.HTTPBadRequest(text=str(exc)) from exc

    app=web.Application(middlewares=[authenticated])
    async def lifecycle(app):
        await host.runtime.start()
        yield
        await host.runtime.close()
    app.cleanup_ctx.append(lifecycle)

    async def submit(request):
        body=await request.json()
        if not isinstance(body,dict) or set(body)-{'run_id','idempotency_key','input'}:
            raise ValueError('Only run_id, idempotency_key and input are accepted')
        context=request['context']
        run=await host.runtime.submit(RunRequest(body.get('run_id',''),context.session_id,
            body.get('idempotency_key',''),body.get('input',''),deadline_seconds=60),context)
        return web.json_response(asdict(run),status=202)

    async def get_run(request):
        run=await host.runtime.get_run(request.match_info['run_id'],request['context'])
        return web.json_response(asdict(run))

    async def events(request):
        events=await host.runtime.events(request.match_info['run_id'],int(request.query.get('after','0')),request['context'])
        return web.json_response([asdict(event) for event in events])

    async def cancel(request):
        run=await host.runtime.cancel(request.match_info['run_id'],request['context'])
        return web.json_response(asdict(run))

    async def tools(request):
        return web.json_response([{**asdict(tool),'effects':list(tool.effects)}
            for tool in await host.catalog.discover(request['context'])])

    async def fixed_catalog(request):
        raise PermissionError('Replio manages a fixed tool catalog; installation and removal are disabled')

    base='/sessions/{session_id}'
    app.router.add_post(base+'/runs',submit)
    app.router.add_get(base+'/runs/{run_id}',get_run)
    app.router.add_get(base+'/runs/{run_id}/events',events)
    app.router.add_post(base+'/runs/{run_id}/cancel',cancel)
    app.router.add_get(base+'/tools',tools)
    app.router.add_post(base+'/tools',fixed_catalog)
    app.router.add_delete(base+'/tools/{source_id}',fixed_catalog)
    return app
