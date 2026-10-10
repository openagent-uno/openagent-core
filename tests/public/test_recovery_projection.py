from __future__ import annotations

import asyncio
import json
import time


def test_journal_recovery_projects_on_a_detached_connection(tmp_path) -> None:
    from openagent_core.memory.db import MemoryDB

    async def exercise() -> None:
        db = MemoryDB(str(tmp_path / "recovery.db"))
        await db.connect()
        try:
            session_id = "telegram-owner-session"
            now = int(time.time())
            runs = [{
                "run_id": "surviving-run",
                "status": "COMPLETED",
                "content": "surviving answer",
                "messages": [
                    {"role": "user", "content": "surviving question"},
                    {"role": "assistant", "content": "surviving answer"},
                ],
                "created_at": now,
            }]
            conn = await db._ensure_connected()
            await conn.execute(
                "INSERT INTO sessions "
                "(session_id, session_type, agent_id, user_id, metadata, runs, "
                "created_at, updated_at) VALUES (?, 'agent', 'a', 'owner', "
                "'{}', ?, ?, ?)",
                (session_id, json.dumps(runs), now, now),
            )
            await conn.commit()
            await db.append_session_event(
                session_id, "user/message", {"text": "lost watch preference"},
            )
            await db.append_session_event(
                session_id, "assistant/message", {"text": "preference noted"},
            )
            await db.append_session_event(
                session_id, "user/message", {"text": "continue"},
            )

            async def shared_projection_would_fail(_session_id: str) -> None:
                raise RuntimeError(
                    "cannot open savepoint - SQL statements in progress"
                )

            db._project_operational_session = shared_projection_would_fail  # type: ignore[method-assign]

            assert await db.recover_session_from_journal(
                session_id, current_text="continue",
            ) == 2
            projected = await (
                await conn.execute(
                    "SELECT role, text FROM session_messages "
                    "WHERE session_id=? ORDER BY sequence",
                    (session_id,),
                )
            ).fetchall()
            projected_pairs = [(row[0], row[1]) for row in projected]
            assert ("user", "lost watch preference") in projected_pairs
            assert ("assistant", "preference noted") in projected_pairs
        finally:
            await db.close()

    asyncio.run(exercise())
