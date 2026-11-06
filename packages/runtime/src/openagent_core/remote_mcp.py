"""Optional, host-authorized remote MCP federation; no identity or transport dependency."""
from __future__ import annotations
import copy
import hashlib
import json
import re
from collections.abc import Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from openagent_core.capabilities import CapabilityUnavailable, ToolDefinition
from openagent_core.contracts import canonical_json

_NAME=re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$')
_ID=re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,255}$')
_DIGEST=re.compile(r'^[a-f0-9]{64}$')
PROTOCOL='mcp-app-resources/1'

@dataclass(frozen=True)
class Topology:
    rows: tuple = ()
    complete: bool = True
    available: bool = True

_discovery = ContextVar('remote_mcp_discovery', default=None)

@contextmanager
def discovery_scope(context, topology):
    token=_discovery.set((context_key(context),topology))
    try:yield
    finally:_discovery.reset(token)


def context_key(context):
    return hashlib.sha256(canonical_json([context.author.key,context.initiator.key,context.authority.key,
        context.tenant_id,context.agent_id,context.session_id,[p.key for p in context.audience],
        list(context.scopes),context.delegation_id,context.deferred]).encode()).hexdigest()


async def catalog(transport, context, selectors=None):
    result = await transport.catalog(context, selectors or {})
    if result is None:
        return Topology(available=False)
    if not isinstance(result,dict) or not isinstance(result.get('servers'),list) or len(result['servers'])>256:
        raise ValueError('Invalid remote MCP catalog')
    if len(json.dumps(result,allow_nan=False).encode())>2*1024*1024:
        raise ValueError('Remote MCP catalog exceeds its bound')
    if not isinstance(result.get('truncated',False),bool):raise ValueError('Invalid topology completeness marker')
    rows=[];seen=set();total=0
    for row in result['servers']:
        if (not isinstance(row,dict) or not isinstance(row.get('host_id'),str) or not _ID.fullmatch(row['host_id'])
                or not isinstance(row.get('generation'),int) or isinstance(row['generation'],bool) or row['generation']<1
                or not isinstance(row.get('server'),str) or not _NAME.fullmatch(row['server'])
                or row.get('resource_protocol') not in (None, PROTOCOL) or not isinstance(row.get('version'),str) or len(row['version'])>200
                or not isinstance(row.get('catalog_digest'),str) or not _DIGEST.fullmatch(row['catalog_digest'])
                or not isinstance(row.get('tools'),list) or len(row['tools'])>512):
            raise ValueError('Invalid remote MCP source')
        identity=(row['host_id'],row['generation'],row['server'])
        if identity in seen:raise ValueError('Ambiguous remote MCP source')
        seen.add(identity);names=set()
        for tool in row['tools']:
            total+=1
            if (total>4096 or not isinstance(tool,dict) or not isinstance(tool.get('name'),str) or not _NAME.fullmatch(tool['name'])
                    or tool['name'] in names or not isinstance(tool.get('description',''),str) or len(tool.get('description',''))>65536
                    or not isinstance(tool.get('input_schema'),dict) or not isinstance(tool.get('_meta',{}),dict)
                    or len(json.dumps(tool.get('_meta',{}),allow_nan=False).encode())>65536):
                raise ValueError('Invalid remote MCP tool')
            names.add(tool['name'])
        rows.append(row)
    return Topology(tuple(rows),complete=not result.get('truncated',False))


class RemoteMcpSource:
    def __init__(self,transport,context,row):
        self.transport,self.owner,self.row=transport,context_key(context),copy.deepcopy(row)
        self.identity=(row['host_id'],row['generation'],row['server'])

    async def _current(self,context,*,cache_allowed=False):
        if context_key(context)!=self.owner:
            raise PermissionError('Remote MCP belongs to another principal/session audience')
        cached=_discovery.get() if cache_allowed else None
        topology=cached[1] if cached and cached[0]==self.owner else None
        if topology is not None and not topology.available:
            raise CapabilityUnavailable('The current remote MCP topology is unavailable')
        matches=[row for row in topology.rows if (row['host_id'],row['generation'],row['server'])==self.identity] if topology else []
        if topology is None or not matches and not topology.complete:
            topology=await catalog(self.transport,context,{key:self.row[key] for key in ('host_id','generation','server')})
            matches=[row for row in topology.rows if (row['host_id'],row['generation'],row['server'])==self.identity]
        if (len(matches)!=1 or matches[0]['catalog_digest']!=self.row['catalog_digest']
                or matches[0]['tools']!=self.row['tools'] or matches[0]['version']!=self.row['version']):
            raise CapabilityUnavailable('The exact remote MCP or provider generation changed')
        return matches[0]

    async def discover(self,context):
        try:row=await self._current(context,cache_allowed=True)
        except (CapabilityUnavailable,PermissionError,ValueError):return ()
        return tuple(ToolDefinition(tool['name'],tool.get('description',''),tool['input_schema'],metadata=tool.get('_meta',{})) for tool in row['tools'])

    async def discover_app(self,context):
        return await self.discover(context)

    def _scope(self):
        return {key:self.row[key] for key in ('host_id','generation','server','catalog_digest')}

    async def call_tool(self,name,arguments,context):
        row=await self._current(context)
        if not isinstance(name,str) or not any(tool['name']==name for tool in row['tools']) or not isinstance(arguments,Mapping):
            raise PermissionError('Tool is not declared by the exact originating MCP')
        result=await self.transport.call(context, self._scope(), name, dict(arguments))
        await self._current(context)
        return result

    async def call_app_tool(self,name,arguments,context):
        return await self.call_tool(name,arguments,context)

    async def read_resource(self,uri,context):
        row=await self._current(context)
        if row.get('resource_protocol') != PROTOCOL:
            raise CapabilityUnavailable('The exact remote source has no resource transport')
        declared=set()
        for tool in row['tools']:
            metadata=tool.get('_meta',{});ui=metadata.get('ui') or {}
            value=ui.get('resourceUri') if isinstance(ui,dict) else None
            value=value or metadata.get('ui/resourceUri')
            if isinstance(value,str):declared.add(value)
        if not isinstance(uri,str) or uri not in declared or not uri.startswith('ui://'):
            raise PermissionError('Resource is not declared by the exact originating MCP')
        result = await self.transport.read(context, self._scope(), uri)
        await self._current(context)
        return result


async def synchronize(owner,context):
    key=context_key(context);wanted=set()
    try:topology=await catalog(owner.transport,context)
    except (PermissionError,ValueError,CapabilityUnavailable):
        return Topology(complete=False,available=False)
    for row in topology.rows:
        identity=canonical_json([key,row['host_id'],row['generation'],row['server']])
        source_id=owner.source_prefix+hashlib.sha256(identity.encode()).hexdigest()[:48]
        wanted.add(source_id);existing=owner.sources.get(source_id)
        if existing is not None and existing.row == row:continue
        if existing is not None:owner.catalog.revoke(source_id)
        elif len(owner.sources)>=4096:raise CapabilityUnavailable('Remote MCP source registration limit reached')
        source=RemoteMcpSource(owner.transport,context,row)
        owner.catalog.register(source_id,source,source,target_label=row['host_id']+' / '+row['server'])
        owner.sources[source_id]=source
    retained=owner.owners.get(key,set())
    if not topology.complete:wanted|=retained
    for source_id in retained-wanted:
        owner.catalog.revoke(source_id);owner.sources.pop(source_id,None)
    if wanted:owner.owners[key]=wanted
    else:owner.owners.pop(key,None)
    return topology


class RemoteMcpRegistry:
    """Optional dynamic sources. Hosts supply current authorization and IO.

    transport.catalog(context, selectors) returns a neutral catalog or None for
    an unavailable feature. transport.call/read receive only exact selectors.
    No host, account, user count, client presence or sharing policy is inferred.
    """
    def __init__(self, capability_catalog, transport, *, source_prefix="remote_mcp_"):
        if not isinstance(source_prefix, str) or not _NAME.fullmatch(source_prefix):
            raise ValueError("Invalid remote capability namespace")
        self.catalog, self.transport, self.source_prefix = capability_catalog, transport, source_prefix
        self.sources, self.owners = {}, {}

    async def synchronize(self, context):
        return await synchronize(self, context)

    def discovery_scope(self, context, topology):
        return discovery_scope(context, topology)
