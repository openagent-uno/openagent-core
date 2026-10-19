"""Workspace services preserve files/history and use fresh host ACLs."""
from __future__ import annotations
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from openagent_core import Runtime,RuntimeSettings,RuntimeServices,PrincipalRef,ExecutionContext,RunRequest
from openagent_core.administration import ManagementContext
from openagent_core.workspace_administration import SkillsLibrary,SessionInspection
from openagent_core.engine import MemoryDB
from openagent_storage_sqlite import SqliteRuntimeStore

class Policy:
    def __init__(self):self.denied=set()
    async def authorize(self,context,action,resource,*,audience=()):return resource.resource_id not in self.denied and context.authority.subject_id=="alice"

class WorkspaceAdministration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)
        self.policy=Policy();self.db=MemoryDB(str(self.path/"state.sqlite"));await self.db.connect()
        self.store=SqliteRuntimeStore(self.path/"state.sqlite");await self.store.start()
        self.alice=PrincipalRef("host","tenant","alice","user")
        self.ctx=ExecutionContext(self.alice,self.alice,self.alice,"parent","agent",(self.alice,))
        self.management=ManagementContext(self.alice,"agent")
        self.runtime=Runtime(RuntimeSettings("agent",self.path,environment=(("OPENAGENT_SKILLS_PATH",str(self.path/"skills")),("OPENAGENT_DB_PATH",str(self.path/"state.sqlite")))),RuntimeServices(self.store,object(),self.policy))
        self.skills=SkillsLibrary(self.runtime,self.policy)
        self.inspection=SessionInspection(self.db,self.policy)
    async def asyncTearDown(self):
        await self.store.close();await self.db.close();self.tmp.cleanup()

    async def test_skill_lifecycle_preserves_body_provenance_and_user_ownership(self):
        result=await self.skills.write(self.management,"create","User Playbook",{"body":"Keep these exact instructions.","description":"Original"})
        self.assertTrue(result["ok"]);self.assertFalse(result["index_refreshed"])
        content=await self.skills.read(self.management,"User Playbook")
        self.assertIn("created_by: user",content["content"])
        await self.skills.write(self.management,"update","User Playbook",{"description":"Changed"})
        self.assertEqual("Keep these exact instructions.",(await self.skills.read(self.management,"User Playbook"))["body"])
        await self.skills.write(self.management,"archive","User Playbook")
        self.assertEqual([], (await self.skills.list(self.management))["skills"])
        await self.skills.write(self.management,"restore","User Playbook")
        self.assertEqual(1,len((await self.skills.list(self.management))["skills"]))
        self.assertEqual(1,(await self.skills.search(self.management,"instructions"))["count"])
        before=Path(content["path"]).read_bytes();self.policy.denied.add("agent")
        with self.assertRaises(PermissionError):await self.skills.write(self.management,"update","User Playbook",{"body":"forged"})
        self.assertEqual(before,Path(content["path"]).read_bytes())

    async def test_skill_source_rejects_slug_collision_and_external_symlink(self):
        await self.skills.write(self.management,"create","Collision!",{"body":"Keep me."})
        with self.assertRaises(ValueError):await self.skills.write(self.management,"create","Collision?",{"body":"Overwrite."})
        outside=self.path/"outside";outside.mkdir();(outside/"SKILL.md").write_text("---\nname: outside\ndescription: private\n---\nPRIVATE SENTINEL\n")
        (self.path/"skills"/"external").symlink_to(outside,target_is_directory=True)
        with self.assertRaises(PermissionError):await self.skills.read(self.management,"outside")

    async def test_inspector_transcript_journal_canonical_child_and_revocation(self):
        await self.store.accept(RunRequest("parent-run","parent","parent-run","Hello"),self.ctx)
        child=self.ctx.child(session_id="child",run_id="parent-run",agent=PrincipalRef("host","tenant","worker","agent"))
        await self.store.accept(RunRequest("child-run","child","child-run","Task"),child)
        await self.db.upsert_session("parent",title="Parent")
        await self.db.upsert_session("child",title="Child",parent_session_id="parent",origin="delegation")
        conn=await self.db._ensure_connected()
        raw=[{"run_id":"parent-run","created_at":1,"messages":[{"role":"user","content":"Hello"},{"role":"assistant","content":"Saved response"}]}]
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'",(json.dumps(raw),));await conn.commit()
        rows=await self.inspection.transcript(self.management,"parent")
        self.assertEqual(["Hello","Saved response"],[m["text"] for m in rows["messages"]])
        self.assertEqual({"ancestries":{"child":["parent"]}},await self.inspection.ancestries("tenant",["child","parent"]))
        self.assertEqual({"ancestries":{}},await self.inspection.ancestries("other",["child"]))
        await self.db.append_session_event("parent","future.nonignorable",{"value":"persisted"})
        journal=await self.inspection.events(self.management,"parent")
        self.assertFalse(journal["diagnostics"]["reconstructable"])
        self.assertEqual(["future.nonignorable"],journal["diagnostics"]["unknown_types"])
        self.policy.denied.add("child")
        self.assertEqual([], (await self.inspection.list(self.management,parent="parent"))["sessions"])
        self.policy.denied.add("parent")
        with self.assertRaises(PermissionError):await self.inspection.transcript(self.management,"parent")

    async def test_transcript_uses_exact_original_and_host_file_ids_without_provider_context(self):
        original = 'Discuss literal <attachment_reference> and [Memory checkpoint (turn 1)]\nDo not redact my text.'
        await self.store.accept(RunRequest("media-run","parent","media",original,
            attachments=({"file_id":"host-file-one"},{"file_id":"host-file-two"})),self.ctx)
        await self.db.upsert_session("parent",title="Attachments")
        conn=await self.db._ensure_connected()
        raw=[{"run_id":"media-run","created_at":1,"messages":[
            {"role":"system","content":"PRIVATE HOST BLOCK"},
            {"role":"user","content":"HISTORY PRIVATE", "from_history":True},
            {"role":"user","content":original+"\n<attachment_reference>PRIVATE PATH AND PREVIEW</attachment_reference>"},
            {"role":"assistant","content":"Compared both files."}]}]
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'",(json.dumps(raw),));await conn.commit()
        result=await self.inspection.transcript(self.management,"parent")
        user=result["messages"][0]
        self.assertEqual(original,user["text"])
        self.assertEqual("media-run",user["runId"])
        self.assertEqual([{"file_id":"host-file-one"},{"file_id":"host-file-two"}],user["attachments"])
        self.assertEqual(["text","attachment","attachment"],[p["kind"] for p in user["parts"]])
        self.assertNotIn("PRIVATE",json.dumps(result))
        live=await self.inspection.live_runs(self.management,"parent")
        self.assertEqual(user["text"],live["runs"][0]["input"])
        self.assertEqual(user["attachments"],live["runs"][0]["attachments"])
        self.assertEqual("Compared both files.",result["messages"][1]["text"])
        # Same legacy identity in another tenant/session never supplies input.
        other=replace(self.ctx,session_id="other")
        await self.store.accept(RunRequest("foreign-run","other","other","FOREIGN SENTINEL"),other)
        raw[0]["run_id"]="foreign-run"
        raw[0]["messages"]=[{"role":"user","content":"Legacy unmodified"}]
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'",(json.dumps(raw),));await conn.commit()
        isolated=await self.inspection.transcript(self.management,"parent")
        self.assertEqual("Legacy unmodified",isolated["messages"][0]["text"])
        self.assertNotIn("FOREIGN SENTINEL",json.dumps(isolated))
        self.assertNotIn("runId",isolated["messages"][0])
        self.policy.denied.add("parent")
        with self.assertRaises(PermissionError):await self.inspection.transcript(self.management,"parent")

    def test_transcript_file_refs_are_path_free_for_both_roles(self):
        from openagent_core.session_transcript import expand_run_messages
        source={"run_id":"media","messages":[{"role":"user","content":"prepared"},
            {"role":"assistant","content":"Done"}],"images":[{"filepath":"/private/old.png"}]}
        refs=[{"artifact_id":"art_"+"a"*32,"artifact_link_id":"link","filename":"result.png",
            "type":"image","mime_type":"image/png","size_bytes":12,"path":"/private/cas",
            "url":"https://untrusted.invalid/file","content":"SECRET"}]
        out=expand_run_messages(source,timestamp=1,msg_counter=[0],
            canonical_inputs={"media":{"text":"User literal <attachment_reference>","runId":"media","attachments":[{"file_id":"opaque-host-id"}]}},
            canonical_messages={("media",1,"assistant"):{"attachments":refs}})
        self.assertEqual("User literal <attachment_reference>",out[0]["text"])
        self.assertEqual([{"file_id":"opaque-host-id"}],out[0]["attachments"])
        self.assertEqual("/api/artifacts/art_"+"a"*32+"/content",out[1]["attachments"][0]["url"])
        self.assertEqual("image",out[1]["attachments"][0]["type"])
        self.assertNotIn("/private",json.dumps(out));self.assertNotIn("untrusted.invalid",json.dumps(out));self.assertNotIn("SECRET",json.dumps(out))

    async def test_canonical_parts_and_cas_media_are_shared_for_user_and_assistant(self):
        from openagent_core.memory.artifacts import persist_output_attachments
        from openagent_core.memory.message_parts import persist_parts_for_latest_message
        from openagent_core.memory.operational.access import AccessContext
        await self.store.accept(RunRequest("parts-run","parent","parts","original middle end"),self.ctx)
        await self.db.upsert_session("parent",title="Media")
        conn=await self.db._ensure_connected()
        raw=[{"run_id":"parts-run","messages":[{"role":"user","content":"provider-only"},
            {"role":"assistant","content":"legacy raw"}]}]
        await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'",(json.dumps(raw),))
        for ordinal,role in enumerate(("user","assistant")):
            await conn.execute("INSERT OR IGNORE INTO session_messages (id,tenant_id,session_id,run_id,sequence,ordinal,role,status,author_kind,author_principal_id,text,visibility,source_version,completeness,raw_envelope_json,raw_envelope_schema,legacy_inferred,created_at_ms,updated_at_ms) VALUES (?,?,?,?,?,?,?,'complete',?,'user:alice',?,'user_visible',1,'complete','{}',1,0,1,1)",
                ("msg-"+role,"tenant","parent","parts-run",ordinal,ordinal,role,"user" if role=="user" else "agent","provider-only"))
        for ordinal, message in enumerate(raw[0]["messages"]):
            await conn.execute("UPDATE session_messages SET raw_envelope_json=? WHERE run_id='parts-run' AND ordinal=?",(json.dumps(message),ordinal))
        await conn.commit()
        access=AccessContext("tenant","user:alice","user","alice","device",frozenset({"user:alice"}),frozenset({("user","alice")}))
        # Parts schema references the optional view revision key even for NULL
        # file-only rows. This fixture installs that empty FK target only.
        await conn.execute("CREATE TABLE IF NOT EXISTS ui_view_revisions(view_id TEXT,revision INTEGER,PRIMARY KEY(view_id,revision))")
        await conn.commit()
        refs=[]
        for kind,mime,suffix in (("file","text/csv","csv"),("image","image/png","png"),("voice","audio/wav","wav"),("video","video/mp4","mp4")):
            file=self.path/("fixture."+suffix);file.write_bytes(b"synthetic "+kind.encode())
            refs.append((await persist_output_attachments(self.db,"parent",[{"path":str(file),"filename":"fixture."+suffix,"type":kind,"mime_type":mime}],principal=None))[0])
        for role in ("user","assistant"):
            await persist_parts_for_latest_message(self.db,"parent",role=role,
                parts=[{"kind":"text","text":"original" if role=="user" else "Canonical response"},{"kind":"attachment","attachment":dict(refs[0])},{"kind":"text","text":" middle "}]+[{"kind":"attachment","attachment":dict(ref)} for ref in refs[1:]]+[{"kind":"text","text":"end"}],principal=None,timeout_seconds=0)
        value=await self.inspection.transcript(self.management,"parent")
        self.assertEqual(["original middle end","Canonical response middle end"],[m["text"] for m in value["messages"]])
        self.assertEqual(["text","attachment","text","attachment","attachment","attachment","text"],[p["kind"] for p in value["messages"][0]["parts"]])
        for message in value["messages"]:
            self.assertEqual(["file","image","voice","video"],[a["type"] for a in message["attachments"]])
            self.assertTrue(all(a.get("artifact_link_id") and a.get("artifact_id") for a in message["attachments"]))
            self.assertNotIn("path",json.dumps(message));self.assertNotIn("content_base64",json.dumps(message))
        await conn.execute("UPDATE artifacts SET deleted_at_ms=created_at_ms+1 WHERE id=?",(refs[0]["artifact_id"],));await conn.commit()
        removed=await self.inspection.transcript(self.management,"parent")
        self.assertTrue(all(len(m["attachments"])==3 for m in removed["messages"]))

    async def test_engine_standard_delivery_uses_explicit_publisher_and_persists_public_output(self):
        from types import SimpleNamespace
        from openagent_core.engine import AgentExecutor
        from openagent_core.artifacts import ArtifactRepository, session_output_reference
        from openagent_core.runtime import execution_scope
        from openagent_core.mcp.servers.attachments.adapters import build_runtime_toolkit
        request=RunRequest("delivery-run","parent","delivery","Return a generated CSV")
        await self.store.accept(request,self.ctx)
        await self.db.upsert_session("parent",title="Delivery")
        outer=self
        conn=await self.db._ensure_connected()
        await conn.executemany("INSERT INTO domain_events(event_id,tenant_id,actor_principal_type,actor_principal_id,resource_type,resource_id,session_id,run_id,event_type,occurred_at_ms,schema_version,metadata_json) VALUES (?,'tenant','user','{}','run','delivery-run','parent','delivery-run','run.stream',1,1,?)",
            [(f"long-stream-{i}",json.dumps({"kind":"delta","text":""})) for i in range(2001)])
        await conn.commit()
        repository=ArtifactRepository(self.db,self.policy)
        class Publisher:
            async def publish(self,context,path):
                outer.assertEqual("/sandbox/generated.csv",path)
                return await repository.upload(context,b"name,total\nBoreal,200\n",filename="generated.csv",
                    mime_type="text/csv",session_id=context.session_id)
        async def run_stream(*args,**kwargs):
            delivered=await build_runtime_toolkit().tools[0]("/sandbox/generated.csv")
            text="Generated. "+delivered["marker"]+" End."
            conn=await outer.db._ensure_connected()
            raw=[{"run_id":"delivery-run","messages":[{"role":"user","content":"Return a generated CSV"},
                {"role":"assistant","content":text}]}]
            await conn.execute("UPDATE sessions SET runs=? WHERE session_id='parent'",(json.dumps(raw),));await conn.commit()
            yield {"kind":"delta","text":text[:18]}
            yield {"kind":"delta","text":text[18:]}
            yield {"kind":"done","text":text}
        agent=SimpleNamespace(memory_db=self.db,run_stream=run_stream)
        executor=AgentExecutor(agent)
        runtime=Runtime(RuntimeSettings("agent",self.path),RuntimeServices(self.store,executor,self.policy))
        runtime.services.registry.bind("attachments.publisher",Publisher())
        with execution_scope(runtime,self.ctx,"delivery-run"):
            result=await executor.execute(request,self.ctx,runtime)
        self.assertEqual("Generated.  End.",result)
        live=await self.inspection.live_runs(self.management,"parent")
        self.assertTrue(live["runs"][0]["truncated"])
        frame=next(e["payload"] for e in live["runs"][0]["events"] if e["payload"].get("kind")=="done")
        self.assertEqual(["text","attachment","text"],[p["kind"] for p in frame["parts"]])
        ref=frame["attachments"][0]
        self.assertEqual(("generated.csv","text/csv","file"),(ref["filename"],ref["mime_type"],ref["type"]))
        self.assertEqual(b"name,total\nBoreal,200\n",(await repository.read(self.ctx,artifact_id=ref["artifact_id"])).content)
        history=await self.inspection.transcript(self.management,"parent")
        self.assertEqual(frame["parts"],history["messages"][-1]["parts"])
        self.assertNotIn("/sandbox",json.dumps(live));self.assertNotIn(str(self.path),json.dumps(live))
        self.assertNotIn("[FILE:",json.dumps(live))
        self.assertIsNone(await session_output_reference(self.db,tenant_id="foreign",session_id="parent",value=ref))
        self.assertIsNone(await session_output_reference(self.db,tenant_id="tenant",session_id="other",value=ref))
        conn=await self.db._ensure_connected()
        relation=await (await conn.execute("SELECT count(*) FROM artifact_links WHERE artifact_id=? AND relation='output_attachment'",(ref["artifact_id"],))).fetchone()
        self.assertEqual(1,relation[0])
        await conn.execute("UPDATE artifacts SET deleted_at_ms=created_at_ms+1 WHERE id=?",(ref["artifact_id"],));await conn.commit()
        deleted_live=await self.inspection.live_runs(self.management,"parent")
        terminal=next(e["payload"] for e in deleted_live["runs"][0]["events"] if e["payload"].get("kind")=="done")
        self.assertEqual([],terminal["attachments"])
        self.assertEqual([], (await self.inspection.transcript(self.management,"parent"))["messages"][-1]["attachments"])
        self.policy.denied.update({"parent",self.alice.key})
        with self.assertRaises(LookupError):await repository.read(self.ctx,artifact_id=ref["artifact_id"])
        with self.assertRaises(PermissionError):await self.inspection.transcript(self.management,"parent")

    async def test_real_stream_turn_keeps_original_input_not_prepared_provider_text(self):
        from types import SimpleNamespace
        from openagent_core.stream.session import StreamSession
        from openagent_core.artifacts import ArtifactRepository
        await self.db.upsert_session("stream-parent",title="Stream",client_id="alice")
        conn=await self.db._ensure_connected()
        row=await (await conn.execute("SELECT tenant_id FROM sessions_v2 WHERE id='stream-parent'")).fetchone()
        actor=PrincipalRef("host",row[0],"alice","user")
        context=ExecutionContext(actor,actor,actor,"stream-parent","agent",(actor,))
        management=ManagementContext(actor,"agent")
        repository=ArtifactRepository(self.db,self.policy)
        upload=await repository.upload(context,b"synthetic workbook",filename="workbook.xlsx",session_id="stream-parent")
        conn=await self.db._ensure_connected()
        await conn.execute("CREATE TABLE IF NOT EXISTS ui_view_revisions(view_id TEXT,revision INTEGER,PRIMARY KEY(view_id,revision))");await conn.commit()
        outer=self
        async def run_stream(*args,**kwargs):
            outer.assertEqual("Original <attachment_reference>",kwargs["message"])
            raw=[{"run_id":"stream-run","messages":[{"role":"system","content":"HOST"},
                {"role":"user","content":"OLD PREPARED", "from_history":True},
                {"role":"user","content":"PROVIDER-ONLY prepared workbook preview"},
                {"role":"assistant","content":"Read workbook"}]}]
            conn=await outer.db._ensure_connected()
            await conn.execute("UPDATE sessions SET runs=? WHERE session_id='stream-parent'",(json.dumps(raw),));await conn.commit()
            await outer.db.upsert_session("stream-parent",title="Stream",client_id="alice")
            yield {"kind":"done","text":"Read workbook"}
        agent=SimpleNamespace(memory_db=self.db,db=self.db,config={},run_stream=run_stream,name="stream-test")
        stream=StreamSession(agent,client_id="alice",session_id="stream-parent",speak_enabled=False)
        try:
            result=await stream.run_one_shot("Original <attachment_reference>",attachments=[upload],speak=False)
            self.assertEqual("Read workbook",result["text"])
        finally:await stream.close()
        rows=(await self.inspection.transcript(management,"stream-parent"))["messages"]
        self.assertEqual("Original <attachment_reference>",rows[0]["text"])
        self.assertTrue(rows[0].get("attachments"))
        self.assertEqual("workbook.xlsx",rows[0]["attachments"][0]["filename"])
        self.assertEqual(["text","attachment"],[p["kind"] for p in rows[0]["parts"]])
        self.assertNotIn("PROVIDER-ONLY",json.dumps(rows));self.assertNotIn("HOST",json.dumps(rows))

    def test_public_parts_never_cross_run_role_or_system_history_slots(self):
        from openagent_core.session_transcript import expand_run_messages
        messages=[{"role":"system","content":"HOST"},{"role":"user","content":"HISTORY","from_history":True},
            {"role":"user","content":"Prepared input"},{"role":"tool","content":"Tool result","name":"read"},
            {"role":"assistant","content":"Reply"}]
        outputs=[]
        for run_id in ("first","second"):
            outputs.append(expand_run_messages({"run_id":run_id,"messages":messages},timestamp=1,msg_counter=[0],
                canonical_inputs={run_id:{"text":"Original "+run_id,"runId":run_id,"attachments":[{"file_id":"file-"+run_id}]}},
                canonical_messages={(run_id,4,"assistant"):{"text":"Answer "+run_id,"attachments":[{"artifact_id":"art_"+("a" if run_id=="first" else "b")*32}]},
                    (run_id,2,"assistant"):{"text":"WRONG ROLE"},("foreign",4,"assistant"):{"text":"OTHER RUN"}}))
        self.assertEqual(["Original first","Tool result","Answer first"],[m["text"] for m in outputs[0]])
        self.assertEqual(["Original second","Tool result","Answer second"],[m["text"] for m in outputs[1]])
        self.assertNotIn("HISTORY",json.dumps(outputs));self.assertNotIn("WRONG ROLE",json.dumps(outputs));self.assertNotIn("HOST",json.dumps(outputs))

    def test_transcript_never_promotes_unverified_child_links(self):
        from openagent_core.session_transcript import expand_run_messages
        run={"tools":[{"tool_name":"delegate_task_to_member","tool_call_id":"call","child_session_id":"private-child","child_run_id":"child-run","result":"Done"}],"messages":[{"role":"tool","name":"delegate_task_to_member","tool_call_id":"call","content":"Done"}],"member_responses":[{"run_id":"child-run","session_id":"private-child","messages":[{"role":"assistant","content":"PRIVATE SENTINEL"}]}]}
        denied=expand_run_messages(run,timestamp=1,msg_counter=[0],parent_session_id="parent")
        self.assertNotIn("PRIVATE SENTINEL",json.dumps(denied));self.assertNotIn("child_session_id",json.dumps(denied))
        allowed=expand_run_messages(run,timestamp=1,msg_counter=[0],parent_session_id="parent",authorized_child_sessions=frozenset({"private-child"}))
        self.assertEqual("private-child",allowed[0]["toolInfo"]["child_session_id"])

if __name__=="__main__":unittest.main()
