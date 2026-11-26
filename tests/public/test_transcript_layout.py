import json
import tempfile
import unittest
from importlib.resources import files
from pathlib import Path

import aiosqlite

from openagent_core import ExecutionContext, PrincipalRef, RunRequest
from openagent_core.memory.operational.access import AccessContext
from openagent_core.memory.transcript_layout import transcript_segments
from openagent_storage_sqlite import SqliteRuntimeStore


class TranscriptLayout(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "history.sqlite"
        self.store = SqliteRuntimeStore(path)
        await self.store.start()
        self.addAsyncCleanup(self.store.close)
        self.conn = await aiosqlite.connect(path)
        self.conn.row_factory = aiosqlite.Row
        self.addAsyncCleanup(self.conn.close)
        self.principal = PrincipalRef("fixture", "tenant", "reader")
        self.access = self.access_for(self.principal.key)

    @staticmethod
    def access_for(principal, tenant="tenant"):
        return AccessContext(
            tenant, principal, "user", "reader", "device",
            frozenset({principal}), frozenset({("user", principal)}),
        )

    async def turn(self, *, suffix="", done=True, finish_tools=True, extra_delta="", final_text=None):
        context = ExecutionContext(self.principal, self.principal, self.principal,
                                   "session", "agent", (self.principal,))
        await self.store.accept(RunRequest("run", "session", "key", "Prompt"), context)
        await self.store.transition("run", "running")
        for call, chunk in (("first", "Prima 🧑🏽‍💻\n"), ("second", "Dopo à")):
            await self.store.append_event("run", "run.stream", {"kind": "delta", "text": chunk})
            await self.store.begin_tool(
                "run", call, {"source_id": "example", "name": "read"}, {"private_input": "hidden"},
            )
        for call in ("second", "first"):
            if finish_tools:
                await self.store.finish_tool("run", call, result={"private_result": "hidden"})
        if suffix or extra_delta:
            await self.store.append_event("run", "run.stream", {"kind": "delta", "text": suffix + extra_delta})
        full_text = "Prima 🧑🏽‍💻\nDopo à" + suffix
        if done:
            await self.store.append_event("run", "run.stream", {"kind": "done", "text": full_text})
        await self.store.transition("run", "success", output=final_text if final_text is not None else full_text)
        return [
            dict(row) for row in self.store.connection.execute(
                "SELECT id, run_id, role FROM session_messages ORDER BY sequence"
            )
        ]

    async def layout(self, page, access=None, session_id="session"):
        return await transcript_segments(
            self.conn, access=self.access if access is None else access, session_id=session_id, messages=page,
        )

    async def test_plain_native_store_pages_split_the_entire_turn_in_order(self):
        user, first, second, assistant = await self.turn()
        expected_first = {"text": "Prima 🧑🏽‍💻\n", "parts": [{"kind": "text", "text": "Prima 🧑🏽‍💻\n"}]}
        expected_second = {"text": "Dopo à", "parts": [{"kind": "text", "text": "Dopo à"}]}
        before, after = await self.layout([first]), await self.layout([second, assistant])
        self.assertEqual(before, {first["id"]: expected_first})
        self.assertEqual(after, {second["id"]: expected_second, assistant["id"]: {"text": "", "parts": []}})
        self.assertEqual(await self.layout([assistant]), {assistant["id"]: {"text": "", "parts": []}})
        self.assertEqual(await self.layout([user, first, second, assistant]), before | after)
        self.assertNotIn("private_", json.dumps(before | after))
        raw = self.store.connection.execute(
            "SELECT text FROM session_messages WHERE id=?", (assistant["id"],)
        ).fetchone()
        self.assertEqual(raw[0], expected_first["text"] + expected_second["text"])

    async def test_rejects_missing_final_frame(self):
        page = await self.turn(done=False)
        self.assertEqual(await self.layout(page), {})

    async def test_rejects_text_missing_from_canonical_answer(self):
        page = await self.turn(extra_delta=" Former private draft")
        self.assertEqual(await self.layout(page), {})

    async def test_rejects_revised_assistant_even_when_text_is_unchanged(self):
        page = await self.turn()
        self.store.connection.execute(
            "UPDATE session_messages SET source_version=source_version+1 WHERE role='assistant'"
        )
        self.assertEqual(await self.layout(page), {})

    async def test_rejects_missing_or_unordered_tool_anchors(self):
        _, first, second, _ = page = await self.turn()
        self.store.connection.executemany(
            "UPDATE session_messages SET tool_call_id=? WHERE id=?",
            [("second", first["id"]), ("first", second["id"])],
        )
        self.assertEqual(await self.layout(page), {})
        self.store.connection.executemany(
            "UPDATE session_messages SET tool_call_id=? WHERE id=?",
            [("first", first["id"]), ("second", second["id"])],
        )
        self.store.connection.execute(
            "UPDATE session_messages SET visibility='hidden' WHERE id=?", (first["id"],)
        )
        self.assertEqual(await self.layout([second]), {})

    async def test_rejects_removed_terminal_tool_event(self):
        page = await self.turn(finish_tools=False)
        self.store.connection.execute("UPDATE session_messages SET status='complete' WHERE role='tool'")
        self.assertEqual(await self.layout(page), {})

    async def test_rejects_foreign_or_revoked_session_access_even_for_tool_only_page(self):
        _, first, _, _ = await self.turn()
        self.assertEqual(await self.layout([first], self.access_for("user:other")), {})
        self.assertEqual(await self.layout([first], self.access_for(self.principal.key, "other")), {})
        self.assertEqual(await self.layout([first], session_id="another"), {})
        self.assertEqual(await self.layout([{**first, "id": "nonexistent"}]), {})

    async def test_optional_canonical_rich_parts_keep_their_order_and_require_current_acl(self):
        a = "Prima 🧑🏽‍💻"
        b = "\nDopo à"
        tail = " / fine"
        raw = a + "[FILE:/private/fixture.txt]" + b + "[OPENAGENT_UI:view_abcdefgh@1]" + tail
        _, first, second, assistant = page = await self.turn(suffix=tail, final_text=raw)
        self.assertEqual(await self.layout(page), {})
        for schema in ("custom_views_v1.sql", "message_parts_v1.sql"):
            await self.conn.executescript(
                files("openagent_core.memory.operational.sql").joinpath(schema).read_text(),
            )
        db = self.store.connection
        db.execute(
            "INSERT INTO artifacts(id,tenant_id,owner_principal_id,direction,kind,original_filename,storage_key,"
            "sha256,size_bytes,storage_state,retention_class,created_at_ms,updated_at_ms) "
            "VALUES('file','tenant',?,'output','file','fixture.txt','storage/fixture',?,7,'available','normal',1,1)",
            (self.principal.key, "0" * 64),
        )
        db.execute(
            "INSERT INTO artifact_links(id,tenant_id,artifact_id,resource_type,resource_id,relation,created_at_ms) "
            "VALUES('link','tenant','file','message',?,'attachment',1)", (assistant["id"],),
        )
        db.execute(
            "INSERT INTO ui_views(id,tenant_id,owner_principal_id,surface,session_id,title,created_at_ms,updated_at_ms) "
            "VALUES('view_abcdefgh','tenant',?,'inline','session','Dashboard',1,1)", (self.principal.key,),
        )
        db.execute(
            "INSERT INTO ui_view_revisions(view_id,tenant_id,revision,bundle_path,bundle_sha256,bundle_size_bytes,"
            "created_by_principal_id,created_at_ms) VALUES('view_abcdefgh','tenant',1,'/private/bundle',?,0,?,1)",
            ("0" * 64, self.principal.key),
        )
        columns = "id,tenant_id,session_id,message_id,ordinal,kind,text_content,artifact_link_id,ui_view_id,ui_revision,created_at_ms"
        db.executemany(
            f"INSERT INTO session_message_parts({columns}) VALUES(?,'tenant','session',?,?,?,?,?,?,?,1)",
            [
                ("p1", assistant["id"], 0, "text", a, None, None, None),
                ("p2", assistant["id"], 1, "attachment", None, "link", None, None),
                ("p3", assistant["id"], 2, "text", b, None, None, None),
                ("p4", assistant["id"], 3, "ui_view", None, None, "view_abcdefgh", 1),
                ("p5", assistant["id"], 4, "text", tail, None, None, None),
            ],
        )
        result = await self.layout([first, second, assistant])
        self.assertEqual(result[first["id"]]["text"], a + "\n")
        self.assertEqual([part["kind"] for part in result[first["id"]]["parts"]], ["text", "attachment", "text"])
        self.assertEqual(result[first["id"]]["parts"][1]["attachment"]["url"], "/api/artifacts/file/content")
        self.assertEqual(result[second["id"]], {"text": "Dopo à", "parts": [{"kind": "text", "text": "Dopo à"}]})
        self.assertEqual(result[assistant["id"]]["text"], tail)
        self.assertEqual([part["kind"] for part in result[assistant["id"]]["parts"]], ["ui_view", "text"])
        self.assertNotIn("/private/", json.dumps(result))
        db.execute("UPDATE ui_views SET owner_principal_id='user:someone-else' WHERE id='view_abcdefgh'")
        self.assertEqual(await self.layout([first, second]), {})


if __name__ == "__main__":
    unittest.main()
