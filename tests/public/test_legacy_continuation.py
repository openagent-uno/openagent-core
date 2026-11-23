"""An attested personal history can continue after a writer name changes."""

import json
import sqlite3
import tempfile
import unittest
from contextlib import asynccontextmanager, contextmanager, closing
from dataclasses import replace
from pathlib import Path

from openagent_core import Runtime, RuntimeServices, RuntimeSettings
from openagent_core.provider_history import previous_model_messages
from openagent_core.contracts import ExecutionContext, PrincipalRef, RunRequest
from openagent_core.memory.db import SCHEMA_SQL
from openagent_core.memory.operational.repository import project_legacy_session
from openagent_storage_sqlite import SqliteRuntimeStore
from openagent_storage_sqlite.legacy_continuation import attest_private_continuations
from openagent_storage_sqlite.migration import prepare_migration


@contextmanager
def fenced():
    yield "offline-test"


class Continuation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.snapshot = root / "sealed"
        state = root / "original"
        state.mkdir()
        original = SqliteRuntimeStore(state / "state.db")
        await original.start()
        q = original.connection
        q.executescript(SCHEMA_SQL)
        tenant = (
            "installation:"
            + q.execute(
                "SELECT db_instance_id FROM operational_storage_state WHERE singleton_id=1"
            ).fetchone()[0]
        )
        self.owner = PrincipalRef("host", tenant, "alice")
        envelope = [
            {
                "run_id": "first-run",
                "status": "completed",
                "messages": [
                    {"role": "user", "content": "prior question"},
                    {"role": "assistant", "content": "prior answer"},
                ],
            }
        ]
        for sid, agent, owner in [
            ("private", "former", "alice"),
            ("unbound", None, "alice"),
            ("foreign", "former", "bob"),
            ("ancestor", "former", "alice"),
            ("documented-child", "former", "alice"),
            ("missing-parent", "former", "alice"),
            ("foreign-author", "former", "alice"),
            ("self-prompt", "former", "alice"),
        ]:
            meta = {"client_id": owner}
            items = envelope
            if sid in ("documented-child", "missing-parent"):
                meta["parent_session_id"] = (
                    "ancestor" if sid == "documented-child" else "absent"
                )
            if sid == "self-prompt":
                items = [
                    {
                        "status": "completed",
                        "messages": [
                            {
                                "role": "user",
                                "author": {"kind": "agent", "handle": "former"},
                                "content": "continued",
                            },
                            {"role": "assistant", "content": "answer"},
                        ],
                    }
                ]
            if sid == "foreign-author":
                items = [
                    {
                        "status": "completed",
                        "messages": [
                            {
                                "role": "user",
                                "author": {"kind": "user", "handle": "bob"},
                                "content": "foreign",
                            },
                            {"role": "assistant", "content": "derived from foreign"},
                        ],
                    }
                ]
            q.execute(
                "INSERT INTO sessions(session_id,session_type,agent_id,metadata,runs,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                (
                    sid,
                    "agent",
                    agent,
                    json.dumps(meta),
                    json.dumps(items),
                    1,
                    1,
                ),
            )
            project_legacy_session(q, sid)
            if sid == "documented-child":
                q.execute(
                    "UPDATE sessions_v2 SET parent_session_id=NULL WHERE id=?", (sid,)
                )
        await original.close()

        @asynccontextmanager
        async def held():
            yield "source-seal"

        async def noop(_):
            pass

        self.candidate = root / "candidate"
        await prepare_migration(
            state,
            self.snapshot,
            self.candidate,
            databases=("state.db",),
            quiesce=held,
            migrate=noop,
        )
        self.path = self.candidate / "state.db"
        self.context = ExecutionContext(
            self.owner, self.owner, self.owner, "private", "current", (self.owner,)
        )

    def attest(self):
        return attest_private_continuations(
            self.snapshot,
            "state.db",
            self.path,
            owner=self.owner,
            target_agent="current",
            approved_sessions={
                "private": "former",
                "unbound": None,
                "foreign": "former",
                "documented-child": "former",
                "missing-parent": "former",
                "foreign-author": "former",
                "self-prompt": "former",
            },
            writer_fence=fenced,
        )

    async def test_private_continuation_rechecks_audience_and_preserves_original_ids(
        self,
    ):
        receipt = self.attest()
        self.assertEqual(
            (
                receipt["requested_sessions"],
                receipt["qualified_sessions"],
                receipt["verified_messages"],
            ),
            (7, 4, 8),
        )
        self.assertEqual(self.attest(), receipt)
        reader = SqliteRuntimeStore(self.path)
        await reader.start()
        self.addAsyncCleanup(reader.close)
        for sid, old in [("private", "former"), ("unbound", None)]:
            ctx = replace(self.context, session_id=sid)
            req = RunRequest("new-" + sid, sid, "new", "next")
            await reader.accept(req, ctx)
            await reader.accept(req, ctx)
            await reader.transition(req.run_id, "running")
            window = await reader.provider_history(
                ctx, req.run_id, limit=128, max_bytes=65536
            )
            self.assertEqual(
                [m.text for m in window], ["prior question", "prior answer"]
            )
            self.assertEqual(
                [
                    m.text
                    for m in await reader.provider_history(
                        ctx, req.run_id, limit=1, max_bytes=65536
                    )
                ],
                ["prior answer"],
            )
            self.assertEqual(
                reader.connection.execute(
                    "SELECT agent_id FROM sessions_v2 WHERE id=?", (sid,)
                ).fetchone()[0],
                old,
            )
            await reader.transition(req.run_id, "success", output="new answer")
        other = PrincipalRef("host", self.owner.tenant_id, "other")
        for ctx in (
            replace(self.context, audience=(self.owner, other)),
            replace(
                self.context,
                initiator=other,
                author=other,
                authority=other,
                audience=(other,),
            ),
            replace(self.context, parent_run_id="untrusted-parent"),
            replace(self.context, agent_id="different-agent"),
        ):
            with self.assertRaises(PermissionError):
                await reader.accept(
                    RunRequest("blocked-" + str(id(ctx)), "private", "blocked", "new"),
                    ctx,
                )
        reader.connection.execute(
            "UPDATE sessions_v2 SET acl_version=acl_version+1 WHERE id='private'"
        )
        with self.assertRaises(PermissionError):
            await reader.accept(
                RunRequest("revoked", "private", "revoked", "new"), self.context
            )

    async def test_tamper_and_wrong_agent_fail_before_historical_text_is_returned(self):
        report = self.attest()
        self.assertGreater(report["verified_messages"], 0)
        reader = SqliteRuntimeStore(self.path)
        await reader.start()
        self.addAsyncCleanup(reader.close)
        req = RunRequest("now", "private", "now", "go")
        await reader.accept(req, self.context)
        await reader.transition("now", "running")
        reader.connection.execute(
            "UPDATE session_messages SET text='different' WHERE session_id='private' AND text='prior answer'"
        )
        with self.assertRaises(PermissionError):
            await reader.provider_history(
                self.context, "now", limit=50, max_bytes=65536
            )

    async def test_documented_parent_is_reverified_inside_the_model_entrypoint(self):
        self.attest()
        store = SqliteRuntimeStore(self.path)
        expected = self.context.authority
        actual = []

        class Policy:
            async def authorize(self, context, action, resource, *, audience=()):
                return (
                    context.authority == expected
                    and context.tenant_id == resource.tenant_id
                )

        class Executor:
            async def execute(self, request, context, runtime):
                history = await previous_model_messages(
                    request.session_id, request.run_id
                )
                actual.extend(
                    message.content for message in history if message.from_history
                )
                return "done"

        runtime = Runtime(
            RuntimeSettings("current", None),
            RuntimeServices(store, Executor(), Policy()),
        )
        await runtime.start()
        self.addAsyncCleanup(runtime.close)
        ctx = replace(self.context, session_id="documented-child")
        await runtime.submit(RunRequest("next", ctx.session_id, "next", "now"), ctx)
        record = await runtime.wait("next", ctx)
        self.assertEqual(record.status, "success")
        self.assertEqual(actual, ["prior question", "prior answer"])
        store.connection.execute(
            "UPDATE sessions_v2 SET acl_version=2 WHERE id='ancestor'"
        )
        with self.assertRaises(PermissionError):
            await store.accept(
                RunRequest("changed", ctx.session_id, "changed", "now"), ctx
            )

    async def test_candidate_divergence_rolls_back_every_binding(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(
                "UPDATE session_messages SET author_device_id='changed' WHERE session_id='documented-child'"
            )
        with self.assertRaisesRegex(ValueError, "sealed evidence"):
            self.attest()
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT count(*) FROM oa_private_legacy_sessions"
                ).fetchone()[0],
                0,
            )
