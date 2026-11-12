"""Credential-free recovery of exact history after inactivity and compaction."""
from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from openagent_core import (ExecutionContext, ModuleCatalog, ModuleConfig, PrincipalRef,
    RunRequest, Runtime, RuntimeProfile, RuntimeServices, RuntimeSettings, SessionRef)
from openagent_core.runtime import execution_scope
from openagent_module_sessions import SESSION_PROMPT, descriptor
from openagent_storage_sqlite import SqliteRuntimeStore


class Policy:
    def __init__(self):
        self.denied = set()

    async def authorize(self, context, action, resource, *, audience=()):
        return resource.tenant_id == context.tenant_id and action not in self.denied


class SessionResumeTools(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.path = Path(self.temporary.name)
        self.policy = Policy()
        principal = PrincipalRef("fixture", "tenant", "alice")
        self.context = ExecutionContext(principal, principal, principal, "conversation", "agent", (principal,))
        await self.open_runtime()

    async def open_runtime(self):
        async def execute(request, context, runtime):
            return "Recorded outcome"
        self.store = SqliteRuntimeStore(self.path / "state.sqlite")
        self.runtime = Runtime(RuntimeSettings("agent", self.path),
            RuntimeServices(self.store, SimpleNamespace(execute=execute), self.policy),
            profile=RuntimeProfile(1, {"sessions": ModuleConfig(frozenset({"service", "agent_tools"}))}),
            module_catalog=ModuleCatalog((descriptor,)))
        await self.runtime.start()

    async def asyncTearDown(self):
        await self.runtime.close()
        self.temporary.cleanup()

    async def record(self, run_id, text, *, context=None):
        context = context or self.context
        await self.runtime.submit(RunRequest(run_id, context.session_id, run_id, text), context)
        self.assertEqual((await self.runtime.wait(run_id, context)).status, "success")

    async def call(self, name, arguments):
        with execution_scope(self.runtime, self.context, None, module_generation=1,
                             capability_revision=self.runtime.capabilities.snapshot_revision()):
            tools = await self.runtime.capabilities.discover(self.context)
            selected = next(tool for tool in tools if tool.name == name)
            return await self.runtime.capabilities.call_tool(selected.tool_ref, arguments, self.context)

    async def test_current_session_read_needs_no_guessed_reference_and_is_paginated(self):
        await self.record("first", "The launch plan is CEDAR; next compare the budgets.")
        await self.record("other", "Unrelated private topic", context=replace(self.context, session_id="other"))
        result = await self.call("sessions_read", {"limit": 1})
        self.assertEqual(SessionRef.parse(result["session"]["session_ref"]).session_id, "conversation")
        self.assertEqual(result["messages"][0]["text"], "The launch plan is CEDAR; next compare the budgets.")
        self.assertIsNotNone(result["next_cursor"])
        tail = await self.call("sessions_read", {"limit": 1, "cursor": result["next_cursor"]})
        self.assertEqual(tail["messages"][0]["text"], "Recorded outcome")
        self.assertIsNone(tail["next_cursor"])
        self.assertIn("before claiming you cannot remember", SESSION_PROMPT.text)

    async def test_four_day_gap_and_runtime_restart_preserve_canonical_history(self):
        await self.record("before", "Resume the CEDAR launch plan after comparing budgets.")
        old = int((time.time() - 4 * 24 * 60 * 60) * 1000)
        self.store.connection.execute("UPDATE sessions_v2 SET created_at_ms=?,updated_at_ms=?,last_activity_at_ms=?", (old, old, old))
        self.store.connection.execute("UPDATE session_messages SET created_at_ms=?,updated_at_ms=?", (old, old))
        self.store.connection.commit()
        await self.runtime.close()
        await self.open_runtime()
        await self.record("after", "Continue that plan")
        result = await self.call("sessions_read", {})
        self.assertEqual([message["text"] for message in result["messages"]], [
            "Resume the CEDAR launch plan after comparing budgets.", "Recorded outcome",
            "Continue that plan", "Recorded outcome"])
        search = await self.call("sessions_search", {"query": "CEDAR", "limit": 5})
        self.assertEqual(len(search["sessions"]), 1)
        self.assertEqual(search["sessions"][0]["session_id"], "conversation")

    async def test_current_read_preserves_read_publish_and_reference_boundaries(self):
        await self.record("first", "Current topic")
        for action in ("session.read", "session.publish"):
            self.policy.denied.add(action)
            with self.subTest(action=action), self.assertRaises(PermissionError):
                await self.call("sessions_read", {})
            self.policy.denied.clear()
        foreign = SessionRef("fixture", "other-tenant", "agent", "conversation")
        with self.assertRaises(PermissionError):
            await self.call("sessions_read", {"session_ref": foreign.token})
        for value in ("", None):
            with self.subTest(value=value), self.assertRaises((TypeError, ValueError)):
                await self.call("sessions_read", {"session_ref": value})

    async def test_confirmed_deliveries_keep_authorization_pagination_and_prompt_bounds(self):
        from openagent_core import ConfirmedDelivery
        reference = SessionRef("fixture", "tenant", "agent", "conversation")
        delivery = ConfirmedDelivery("telegram", "bot", "chat", "first", "Sent reminder", 1)
        for action in ("run.submit", "run.publish"):
            self.policy.denied.add(action)
            with self.subTest(action=action), self.assertRaises(PermissionError):
                await self.runtime.record_delivery(delivery, self.context)
            self.policy.denied.clear()
        await self.runtime.record_delivery(delivery, self.context)
        for index in range(25):
            await self.runtime.record_delivery(ConfirmedDelivery("telegram", "bot", "chat", str(index),
                "bounded " + str(index) + " " + "é" * 2000, index + 2), self.context)
        messages, cursor = [], None
        while True:
            page = await self.call("sessions_read", {"limit": 3, **({"cursor": cursor} if cursor else {})})
            messages.extend(page["messages"])
            cursor = page["next_cursor"]
            if cursor is None:
                break
        self.assertEqual(len(messages), 26)
        self.assertTrue(all(row["role"] == "assistant" for row in messages))
        recent = await self.store.delivery_context(reference, self.context, max_bytes=4096)
        self.assertTrue(recent)
        self.assertTrue(recent[-1]["text"].startswith("bounded 24 "))
        self.assertTrue(recent[-1]["truncated"])
        self.assertLessEqual(len(json.dumps(recent, ensure_ascii=False).encode()), 4096)
        foreign = replace(reference, tenant_id="other-tenant")
        with self.assertRaises(PermissionError):
            await self.store.delivery_context(foreign, self.context)
        other = replace(self.context, session_id="another-chat")
        self.assertEqual(await self.store.delivery_context(replace(reference, session_id=other.session_id), other), ())

    async def test_lossy_model_buffer_compaction_keeps_native_history_recoverable(self):
        from openagent_core.engine import MemoryDB
        from openagent_core.core import compaction

        original = "The exact agreed budget is 1739; launch plan CEDAR."
        await self.record("first", original)
        db = MemoryDB(str(self.path / "state.sqlite"))
        await db.connect()
        try:
            await db.upsert_session("conversation", title="Launch planning")
            runs = [{"run_id": "first", "status": "COMPLETED", "messages": [
                {"role": "user", "content": original}, {"role": "assistant", "content": "Recorded outcome"}]}]
            await db._conn.execute("UPDATE sessions SET runs=? WHERE session_id=?", (json.dumps(runs), "conversation"))
            await db._conn.commit()
            agent = SimpleNamespace(_db=db)
            with execution_scope(self.runtime, self.context, None), patch.object(
                    compaction, "_summarize_runs", AsyncMock(return_value="Continue launch planning; the exact budget was omitted.")), patch.object(
                    compaction, "_estimate_text_tokens", side_effect=lambda text, model: max(1, len(text) // 4)):
                result = await compaction.compact("conversation", SimpleNamespace(model="fixture"), agent, keep=0)
            self.assertIsNotNone(result)
            buffer = compaction._load_runs(str(self.path / "state.sqlite"), "conversation")
            self.assertEqual(len(buffer), 1)
            self.assertNotIn("1739", buffer[0]["content"])
            self.assertIn("recap may omit details", buffer[0]["messages"][0]["content"])
            recovered = await self.call("sessions_read", {})
            self.assertEqual(recovered["messages"][0]["text"], original)
        finally:
            await db.close()
