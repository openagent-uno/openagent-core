"""Host-authorized library and inspection services, independent of an HTTP product."""
from __future__ import annotations
import asyncio
import json
from pathlib import Path
import tempfile
import os
from .contracts import ResourceRef, require_authorized
from .runtime import runtime_scope


def require_session_not_quarantined(row):
    """Quarantine is a hard read boundary, even for a permissive host policy.

    Historical identity repair never grants an owner or an audience. A later,
    independently authorized resolution is required before normal inspection.
    """
    if row is not None and row['visibility'] == 'quarantined':
        raise PermissionError('Quarantined historical session is not readable')


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
        require_session_not_quarantined(row)
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

    async def _public_input_refs(self, context, session_id, values):
        from .session_transcript import public_transcript_attachments
        from .artifacts import session_output_reference
        result = []
        for value in values if isinstance(values,(list,tuple)) else ():
            if not isinstance(value,dict):
                continue
            if value.get("artifact_id"):
                ref = await session_output_reference(self.db,tenant_id=context.tenant_id,session_id=session_id,value=value)
                if ref is not None:
                    result.append(ref)
            else:
                result.extend(public_transcript_attachments([value]))
        return result

    async def _public_output_frame(self, context, session_id, frame, *, run_status=None):
        if not isinstance(frame,dict) or frame.get("kind") != "done":
            return frame
        from .artifacts import session_output_reference
        from .stream.content_parts import parse_response_content
        parts = frame.get("parts")
        if not isinstance(parts,list):
            parsed = parse_response_content(frame.get("text", ""),allow_inline_ui=False)
            parts = list(parsed.parts)
        public = []
        for part in parts[:512]:
            if not isinstance(part,dict):
                continue
            if part.get("kind")=="text" and isinstance(part.get("text"),str):
                public.append({"kind":"text","text":part["text"]})
            elif part.get("kind")=="attachment":
                ref=await session_output_reference(self.db,tenant_id=context.tenant_id,session_id=session_id,value=part.get("attachment"))
                if ref is not None:
                    public.append({"kind":"attachment","attachment":ref})
        from .stream.final_answer import canonical_final_answer
        from .contracts import TERMINAL_STATUSES
        failed = frame.get("errored") or (run_status in TERMINAL_STATUSES and run_status != "success")
        answer = None if failed else frame.get("final_answer")
        final = None
        if isinstance(answer,dict) and isinstance(answer.get("text"),str):
            selected, unavailable = [], False
            values = answer.get("attachments", ())
            if not isinstance(values,(list,tuple)):
                values, unavailable = (), True
            # The engine already selected the attested final boundary. A live
            # read must reauthorize its own refs, never reattach transcript
            # input/tool previews to an otherwise text-only final answer.
            for value in values[:256]:
                ref = await session_output_reference(self.db,tenant_id=context.tenant_id,
                    session_id=session_id,value=value)
                if ref is None:
                    unavailable = True
                else:
                    selected.append(ref)
            final = canonical_final_answer(answer["text"],selected,unavailable=unavailable)
        return {**frame,"text":"".join(p["text"] for p in public if p["kind"]=="text").strip(),
            "parts":public,"attachments":[p["attachment"] for p in public if p["kind"]=="attachment"],
            "final_answer":final}

    async def live_runs(self,context,session_id,*,limit=8):
        """Bounded canonical stream snapshots, without authority/credential data.

        This is an observation API; reading or reconnecting never executes a
        run. Authorization is checked before data access and before publication.
        """
        await self.authorize(context,session_id)
        conn=await self.db._ensure_connected()
        rows=await (await conn.execute("SELECT id,status,input_json,output_json,created_at_ms,finished_at_ms,metadata_json FROM session_runs WHERE tenant_id=? AND session_id=? AND json_extract(metadata_json,'$.runtime_contract')=1 ORDER BY ordinal DESC LIMIT ?",(context.tenant_id,session_id,max(1,min(int(limit),8))))).fetchall()
        result=[];revision=0
        for row in reversed(rows):
            metadata=json.loads(row["metadata_json"])
            author=metadata.get("execution_context",{}).get("author",{})
            events=await (await conn.execute("SELECT sequence,event_type,metadata_json,occurred_at_ms FROM domain_events WHERE tenant_id=? AND session_id=? AND run_id=? ORDER BY sequence LIMIT 2001",(context.tenant_id,session_id,row["id"]))).fetchall()
            truncated = len(events) > 2000
            if truncated:
                terminal = await (await conn.execute(
                    "SELECT sequence,event_type,metadata_json,occurred_at_ms FROM domain_events "
                    "WHERE tenant_id=? AND session_id=? AND run_id=? AND event_type='run.stream' "
                    "AND json_extract(metadata_json,'$.kind')='done' ORDER BY sequence DESC LIMIT 1",
                    (context.tenant_id,session_id,row["id"]))).fetchone()
                if terminal is not None and terminal["sequence"] > events[1999]["sequence"]:
                    events = events[:1999] + [terminal]
            # A monotonically increasing journal cursor survives reconnects and
            # restarts. Large streams retain their final canonical output too.
            latest=await (await conn.execute("SELECT max(sequence) FROM domain_events WHERE tenant_id=? AND run_id=?",(context.tenant_id,row["id"]))).fetchone()
            revision=max(revision,int(latest[0] or 0))
            from .session_transcript import public_transcript_attachments
            result.append({"id":row["id"],"status":row["status"],"input":json.loads(row["input_json"]),
                "attachments":await self._public_input_refs(context,session_id,metadata.get("attachments", ())),
                "output":json.loads(row["output_json"]) if row["output_json"] else None,
                "created_at_ms":row["created_at_ms"],"finished_at_ms":row["finished_at_ms"],
                "author":{key:author[key] for key in ("authority","tenant_id","subject_id","kind") if isinstance(author.get(key),str)},
                "events":[{"cursor":e["sequence"],"kind":e["event_type"],"payload":await self._public_output_frame(context,session_id,json.loads(e["metadata_json"]),run_status=row["status"]),"timestamp_ms":e["occurred_at_ms"]} for e in events[:2000] if e["event_type"] in {"run.stream","run.status","run.delta"}],"truncated":truncated})
        await self.authorize(context,session_id)
        return {"session_id":session_id,"revision":revision,"runs":result}

    async def _public_transcript_sources(self, context, session_id, runs):
        """Exact tenant/session/run linkage; no positional prompt guessing."""
        from .session_transcript import public_transcript_attachments
        from .memory.artifacts import attachment_refs_for_messages_on_connection
        from .memory.message_parts import canonical_parts_for_messages_on_connection
        conn = await self.db._ensure_connected()
        inputs, messages = {}, {}
        for run in runs:
            run_id = str(run.get("run_id") or "")
            row = await (await conn.execute(
                "SELECT id,input_json,metadata_json FROM session_runs WHERE tenant_id=? AND session_id=? AND "
                "(id=? OR (id=? AND (json_extract(raw_envelope_json,'$.run_id')=? OR json_extract(raw_envelope_json,'$.id')=?))) "
                "ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END LIMIT 1",
                (context.tenant_id,session_id,run_id,f"run:{session_id}:{run_id}",run_id,run_id,run_id))).fetchone()
            if row is not None:
                metadata = json.loads(row["metadata_json"] or "{}")
                if metadata.get("runtime_contract") == 1 and row["input_json"] is not None:
                    original = json.loads(row["input_json"])
                    if isinstance(original, str):
                        inputs[run_id] = {"text": original, "runId": run_id,
                            "attachments": await self._public_input_refs(context,session_id,metadata.get("attachments", ()))}
            normalized = await (await conn.execute(
                "SELECT id,ordinal,role,raw_envelope_json FROM session_messages WHERE run_id=? AND tenant_id=? AND session_id=? AND visibility='user_visible' ORDER BY ordinal",
                ((row["id"] if row is not None else run_id), context.tenant_id, session_id))).fetchall()
            # Projection ordinal is run-local (projection.py enumerates the
            # original raw list), including system/history/tool slots. Verify
            # role, plus a source message ID when the provider supplied one.
            # Provider content is prepared for inference and deliberately differs
            # from the canonical original; text equality is not identity.
            raw_messages = run.get("messages") or []
            targets = {}
            verified = []
            for message in normalized:
                source = json.loads(message["raw_envelope_json"])
                ordinal = message["ordinal"]
                if row is not None and json.loads(row["metadata_json"] or "{}").get("runtime_contract") == 1 and "author" in source and "role" not in source:
                    # RuntimeStore writes one canonical user input and final
                    # answer, independently of provider history/tool slots.
                    candidates = [i for i,m in enumerate(raw_messages) if isinstance(m,dict)
                        and m.get("role")==message["role"] and not m.get("from_history") and not m.get("tool_calls")]
                    if message["role"] not in {"user","assistant"} or not candidates:
                        continue
                    ordinal = candidates[0] if message["role"]=="user" else candidates[-1]
                if not 0 <= ordinal < len(raw_messages) or not isinstance(raw_messages[ordinal],dict):
                    continue
                raw = raw_messages[ordinal]
                if raw.get("role") != message["role"] or (source.get("id") and raw.get("id") and source["id"] != raw["id"]):
                    continue
                verified.append(message)
                targets[message["id"]] = ordinal
            normalized = verified
            identifiers = [m["id"] for m in normalized]
            refs = await attachment_refs_for_messages_on_connection(conn, identifiers)
            # The host authorizer has checked this exact session. CAS parts
            # inherit that message link; without a native AccessContext,
            # custom views remain hidden rather than acquiring inferred ACLs.
            ordered = await canonical_parts_for_messages_on_connection(conn, identifiers, access=None)
            for message in normalized:
                parts = ordered.get(message["id"])
                if parts is None and message["id"] not in refs:
                    continue
                public = {"attachments": refs.get(message["id"], [])}
                if parts is not None and message["role"] in {"user", "assistant"}:
                    public = {"text": "".join(p["text"] for p in parts if p["kind"] == "text"),
                              "parts": parts,
                              "attachments": [p["attachment"] for p in parts if p["kind"] == "attachment"]}
                messages[(run_id, targets[message["id"]], message["role"])] = public
            # AgentExecutor's terminal parts live in the existing exact-run
            # journal. Revalidate every CAS reference against current session
            # links; a revoked/deleted artifact never returns through history.
            terminal = await (await conn.execute(
                "SELECT metadata_json FROM domain_events WHERE tenant_id=? AND session_id=? AND run_id=? "
                "AND event_type='run.stream' AND json_extract(metadata_json,'$.kind')='done' "
                "ORDER BY sequence DESC LIMIT 1",(context.tenant_id,session_id,run_id))).fetchone()
            if terminal is not None:
                frame = await self._public_output_frame(context,session_id,json.loads(terminal[0]))
                if isinstance(frame.get("parts"),list):
                    final = next((i for i in range(len(raw_messages)-1,-1,-1)
                        if isinstance(raw_messages[i],dict) and raw_messages[i].get("role")=="assistant"
                        and not raw_messages[i].get("from_history") and not raw_messages[i].get("tool_calls")),None)
                    if final is not None:
                        messages[(run_id,final,"assistant")] = {key:frame[key] for key in ("text","parts","attachments")}

        return inputs, messages

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
        originals, canonical = await self._public_transcript_sources(context, session_id, runs)
        messages=[];counter=[0]
        for run in reversed(runs):messages.extend(expand_run_messages(run,timestamp=int(run.get("created_at",0) or 0),msg_counter=counter,parent_session_id=session_id,child_by_run_id=child_by_run,authorized_child_sessions=frozenset(allowed_children),canonical_inputs=originals,canonical_messages=canonical))
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
            value=await (await conn.execute("SELECT id,tenant_id,parent_session_id,origin FROM sessions_v2 WHERE id=? AND visibility<>'quarantined' AND deleted_at_ms IS NULL",(sid,))).fetchone()
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
