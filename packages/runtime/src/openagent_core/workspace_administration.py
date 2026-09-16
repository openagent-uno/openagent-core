"""Host-authorized library and inspection services, independent of an HTTP product."""
from __future__ import annotations
import asyncio
import json
from pathlib import Path
import tempfile
import os
from .contracts import ResourceRef, require_authorized
from .runtime import runtime_scope


class SkillsLibrary:
    """One explicitly configured, host-managed corpus; no private note claims."""
    def __init__(self, runtime, authorizer):
        self.runtime, self.authorizer = runtime, authorizer
        configured=dict(runtime.settings.environment).get("OPENAGENT_SKILLS_PATH")
        if not configured or not Path(configured).is_absolute(): raise ValueError("An explicit absolute skills root is required")
        self.root=Path(configured).resolve()
        self._writes=asyncio.Lock()

    async def authorize(self,context,action):
        await require_authorized(self.authorizer,context,action,ResourceRef("agent",context.tenant_id,context.agent_id))

    def registry(self):
        from .mcp.servers.skills.registry import SkillsRegistry
        registry=SkillsRegistry(self.root);registry.load()
        return registry

    def checked(self,meta):
        if not meta.path.resolve().is_relative_to(self.root): raise PermissionError("Skill escapes its registered source")
        return meta

    @staticmethod
    def metadata(meta):
        return {"name":meta.name,"description":meta.description,"category":meta.category,"path":str(meta.path),
            "created_by":meta.created_by,"status":meta.status,"agent_authored":meta.is_agent_authored,
            "archived":meta.is_archived,"from_hub":meta.is_hub}

    async def list(self,context,*,include_archived=False):
        await self.authorize(context,"skills.read")
        rows=[self.metadata(self.checked(m)) for m in self.registry().skills() if include_archived or not m.is_archived]
        await self.authorize(context,"skills.read")
        return {"skills":sorted(rows,key=lambda r:(r["category"] or "",r["name"]))}

    async def read(self,context,name):
        await self.authorize(context,"skills.read")
        meta=self.registry().get(name)
        if meta is None:raise LookupError("Skill not found")
        self.checked(meta)
        from .mcp.servers.skills.handlers import skill_view
        with runtime_scope(self.runtime):result=await skill_view(name)
        await self.authorize(context,"skills.read")
        return result

    async def search(self,context,query,*,limit=20):
        await self.authorize(context,"skills.read")
        if not isinstance(query,str) or not query.strip():raise ValueError("Search query is required")
        for meta in self.registry().skills():self.checked(meta)
        from .mcp.servers.skills.handlers import skill_search
        with runtime_scope(self.runtime):result=await skill_search(query,max(1,min(int(limit),100)))
        await self.authorize(context,"skills.read")
        return result

    async def write(self,context,action,name,fields=None):
        await self.authorize(context,"skills.write")
        if action not in {"create","update","remove","archive","restore"}:raise ValueError("Unknown skill operation")
        if not isinstance(name,str) or not name.strip() or len(name)>200:raise ValueError("Invalid skill name")
        fields=dict(fields or {})
        if set(fields)-{"body","description","category"} or any(not isinstance(v,str) for v in fields.values()):raise ValueError("Invalid skill fields")
        if action not in {"create","update"} and fields:raise ValueError("This operation has no editable fields")
        from .mcp.servers.skills.handlers import skill_manage, _slug, _skill_markdown, _preserved_provenance
        from .memory.vault.parser import split_frontmatter
        async with self._writes:
            await self.authorize(context,"skills.write")
            existing=self.registry().get(name)
            if existing:self.checked(existing)
            if action!="create" and existing is None:raise LookupError("Skill not found")
            if action=="create" and (existing is not None or (self.root/_slug(name)).exists()):raise ValueError("A skill already occupies this name")
            if action=="restore":
                _,body=split_frontmatter(existing.path.read_text())
                extra=_preserved_provenance(existing);extra.pop("status",None)
                content=_skill_markdown(existing.name,existing.description,existing.category,body,extra=extra)
                self._replace(existing.path,content)
                result={"ok":True,"action":"restore","name":name,"path":str(existing.path)}
            else:
                with runtime_scope(self.runtime):result=await skill_manage(action,name,**fields)
                if action=="create" and result.get("ok") and context.authority.kind=="user":
                    # A trusted management request is human-authored. Curators
                    # must not treat it as an autonomous agent's editable draft.
                    created=self.checked(self.registry().get(name))
                    _,body=split_frontmatter(created.path.read_text())
                    self._replace(created.path,_skill_markdown(created.name,created.description,created.category,body,extra={"created_by":"user"}))
            result["index_refreshed"]=False
            return result

    @staticmethod
    def _replace(path,content):
        fd,temp=tempfile.mkstemp(prefix=".skill-",dir=path.parent)
        try:
            with os.fdopen(fd,"w") as file:file.write(content);file.flush();os.fsync(file.fileno())
            os.replace(temp,path)
        finally:
            if os.path.exists(temp):os.unlink(temp)


class SessionInspection:
    def __init__(self,db,authorizer,*,agent=None):
        self.db,self.authorizer,self.agent=db,authorizer,agent

    async def authorize(self,context,session_id):
        await require_authorized(self.authorizer,context,"session.read",ResourceRef("session",context.tenant_id,session_id))
        conn=await self.db._ensure_connected()
        row=await (await conn.execute("SELECT * FROM sessions_v2 WHERE id=? AND deleted_at_ms IS NULL",(session_id,))).fetchone()
        if row is not None and row["tenant_id"] != context.tenant_id:raise PermissionError("Session tenant does not match host provenance")
        return dict(row) if row is not None else None

    async def list(self,context,*,parent=None,limit=50):
        await require_authorized(self.authorizer,context,"session.list",ResourceRef("agent",context.tenant_id,context.agent_id))
        if parent:await self.authorize(context,parent)
        conn=await self.db._ensure_connected()
        tenants=(context.tenant_id,)
        placeholders=','.join('?' for _ in tenants)
        sql=f"SELECT * FROM sessions_v2 WHERE tenant_id IN ({placeholders}) AND deleted_at_ms IS NULL"
        params=list(tenants)
        sql+=" AND parent_session_id=?" if parent else " AND parent_session_id IS NULL"
        if parent:params.append(parent)
        rows=await (await conn.execute(sql+" ORDER BY last_activity_at_ms DESC LIMIT 2000",params)).fetchall()
        result=[]
        for row in rows:
            try:await self.authorize(context,row["id"])
            except PermissionError:continue
            legacy=await self.db.get_session(row["id"])
            result.append(legacy or {"session_id":row["id"],"title":row["title"],"origin":row["origin"],"parent_session_id":row["parent_session_id"],"kind":row["kind"],"created_at":row["created_at_ms"]/1000,"last_active_at":row["last_activity_at_ms"]/1000})
            if len(result)>=max(1,min(int(limit),200)):break
        return {"sessions":result}

    async def transcript(self,context,session_id,*,limit=20):
        await self.authorize(context,session_id)
        runs=await self.db.list_session_runs(session_id,limit=max(1,min(int(limit),100)))
        conn=await self.db._ensure_connected()
        children=await (await conn.execute("SELECT id FROM sessions_v2 WHERE parent_session_id=? AND deleted_at_ms IS NULL",(session_id,))).fetchall()
        child_by_run={};allowed_children=set()
        legacy_children=await self.db.list_child_sessions(session_id)
        for child in children:
            try:await self.authorize(context,child["id"])
            except PermissionError:continue
            allowed_children.add(child["id"])
            # The legacy row supplies display linkage only after the canonical
            # parent and current child authorization have both been verified.
            for row in legacy_children:
                if row["session_id"]==child["id"] and row.get("child_run_id"):child_by_run[row["child_run_id"]]=child["id"]
        from .session_transcript import expand_run_messages
        messages=[];counter=[0]
        for run in reversed(runs):messages.extend(expand_run_messages(run,timestamp=int(run.get("created_at",0) or 0),msg_counter=counter,parent_session_id=session_id,child_by_run_id=child_by_run,authorized_child_sessions=frozenset(allowed_children)))
        await self.authorize(context,session_id)
        return {"session_id":session_id,"messages":messages}

    async def events(self,context,session_id,*,after=0,limit=500):
        await self.authorize(context,session_id)
        if int(after)<0:raise ValueError("Invalid event cursor")
        rows=await self.db.list_session_events(session_id,after_seq=int(after),limit=max(1,min(int(limit),2000)))
        known=self.db.JOURNAL_KNOWN_TYPES;ignorable=self.db.JOURNAL_IGNORABLE_TYPES
        unknown=sorted({e["type"] for e in rows if e["type"] not in known and e["type"] not in ignorable})
        tools={}
        for event in rows:
            if event["type"]!="tool/status":continue
            try:info=json.loads((event.get("data") or {}).get("text") or "{}")
            except (TypeError,ValueError):continue
            if isinstance(info,dict) and info.get("tool_name"):tools[info["tool_name"]]=tools.get(info["tool_name"],0)+(-1 if "result" in info else 1)
        await self.authorize(context,session_id)
        return {"session_id":session_id,"events":rows,"last_seq":rows[-1]["seq"] if rows else int(after),"diagnostics":{"unknown_types":unknown,"reconstructable":not unknown,"unpaired_tool_calls":sorted(n for n,c in tools.items() if c>0)}}

    async def context_report(self,context,session_id):
        await self.authorize(context,session_id)
        if self.agent is None:raise LookupError("Agent unavailable")
        from .core.context_report import build_context_report
        report=build_context_report(self.agent,session_id)
        await self.authorize(context,session_id)
        return report or {"session_id":session_id,"sections":[],"context_window":0}

    async def ancestries(self,tenant_id,session_ids):
        """Trusted host attestation endpoint; callers must authenticate workload."""
        if len(session_ids)>64 or len(set(session_ids))!=len(session_ids):raise ValueError("Invalid ancestry batch")
        conn=await self.db._ensure_connected();result={}
        async def row(sid):
            value=await (await conn.execute("SELECT id,tenant_id,parent_session_id,origin FROM sessions_v2 WHERE id=? AND deleted_at_ms IS NULL",(sid,))).fetchone()
            return value if value is not None and value["tenant_id"] == tenant_id else None
        for sid in session_ids:
            current=await row(sid)
            if current is None or current["origin"]=="chat" or not current["parent_session_id"]:continue
            chain=[];seen={sid}
            for _ in range(16):
                parent=current["parent_session_id"]
                if not parent or parent in seen:break
                current=await row(parent)
                if current is None:break
                chain.append(parent);seen.add(parent)
                if not current["parent_session_id"]:result[sid]=chain;break
        return {"ancestries":result}
