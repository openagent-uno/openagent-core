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

    def test_transcript_never_promotes_unverified_child_links(self):
        from openagent_core.session_transcript import expand_run_messages
        run={"tools":[{"tool_name":"delegate_task_to_member","tool_call_id":"call","child_session_id":"private-child","child_run_id":"child-run","result":"Done"}],"messages":[{"role":"tool","name":"delegate_task_to_member","tool_call_id":"call","content":"Done"}],"member_responses":[{"run_id":"child-run","session_id":"private-child","messages":[{"role":"assistant","content":"PRIVATE SENTINEL"}]}]}
        denied=expand_run_messages(run,timestamp=1,msg_counter=[0],parent_session_id="parent")
        self.assertNotIn("PRIVATE SENTINEL",json.dumps(denied));self.assertNotIn("child_session_id",json.dumps(denied))
        allowed=expand_run_messages(run,timestamp=1,msg_counter=[0],parent_session_id="parent",authorized_child_sessions=frozenset({"private-child"}))
        self.assertEqual("private-child",allowed[0]["toolInfo"]["child_session_id"])

if __name__=="__main__":unittest.main()
