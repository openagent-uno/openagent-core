import asyncio
import json
from types import SimpleNamespace

import pytest

from openagent_core.automation import AutomationRepository, build_automation_toolkit
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.core.tool_output import DEFAULT_MAX_TOOL_RESULT_CHARS
from openagent_core.memory.db import SCHEMA_SQL
from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.runtime import execution_scope


def test_scheduler_tool_finds_middle_task_in_catalog_with_long_prompts(tmp_path):
    repository = AutomationRepository(tmp_path / "agent.db")
    toolkit = build_automation_toolkit("scheduled_task")
    principal = PrincipalRef("openagent", "network", "alice")
    context = ExecutionContext(
        principal, principal, principal, "session", "agent", (principal,),
        ingress_id="verified",
    )
    prompt = "Long scheduled instructions. " * 100
    definitions = [
        (f"task-{index}", f"Task {index}", "0 9 * * *", prompt, 1, index, index)
        for index in range(32)
    ]
    definitions[10] = ("digest", "Übersicht Digest", "0 9 * * *", prompt, 1, 10, 10)
    definitions[9] = ("paused", "Übersicht Digest (paused)", "0 9 * * *", prompt, 0, 9, 9)

    class Management:
        async def call(self, kind, operation, arguments, scope):
            return await repository.call(kind, operation, arguments, scope, atomic=False)

    async def exercise():
        async with repository.session() as connection:
            await connection.executescript(SCHEMA_SQL)
            await connection.executemany(
                "INSERT INTO scheduled_tasks "
                "(id, name, cron_expression, prompt, enabled, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                definitions,
            )
            await connection.commit()

        runtime = SimpleNamespace(services=SimpleNamespace(automation_management=Management()),
                                  settings=SimpleNamespace(environment=()))
        list_tasks = toolkit.async_functions["list_scheduled_tasks"].entrypoint
        get_task = toolkit.async_functions["get_scheduled_task"].entrypoint
        with execution_scope(runtime, context, "run"):
            page = await list_tasks()
            assert len(page["results"]) == 20
            assert "digest" not in {task["id"] for task in page["results"]}
            tail = await list_tasks(cursor=page["next_cursor"])
            assert tail["next_cursor"] is None
            assert "digest" in {task["id"] for task in tail["results"]}
            catalog = page["results"] + tail["results"]
            assert len({task["id"] for task in catalog}) == len(definitions)
            assert all("prompt" not in task for task in catalog)
            assert len(json.dumps(page)) < DEFAULT_MAX_TOOL_RESULT_CHARS
            assert len(json.dumps(definitions)) > DEFAULT_MAX_TOOL_RESULT_CHARS

            matches = await list_tasks(query="  ÜBERSICHT digest  ", limit=1)
            assert [(task["id"], task["enabled"]) for task in matches["results"]] == [("digest", True)]
            paused = await list_tasks(query="übersicht digest", cursor=matches["next_cursor"])
            assert [(task["id"], task["enabled"]) for task in paused["results"]] == [("paused", False)]
            assert paused["next_cursor"] is None
            with pytest.raises(ValueError, match="Invalid page cursor"):
                await list_tasks(query="missing", cursor=matches["next_cursor"])
            active = await list_tasks(True, query="übersicht")
            assert [task["id"] for task in active["results"]] == ["digest"]
            assert await list_tasks(query="missing") == {"results": [], "next_cursor": None}
            assert (await get_task("digest"))["prompt"] == prompt
            detailed = await list_tasks(query="übersicht", enabled_only=True, include_prompt=True)
            assert detailed == {"results": [await get_task("digest")], "next_cursor": None}

            descriptor = NativeCapabilityDescriptor(
                id="scheduler", version="1.1.0b1", native_sources=("scheduler",),
                source_names={"scheduler": "schedules"}, search_source="schedules",
                search_operation="list_scheduled_tasks", search_accepts_limit=False,
            )
            module = await descriptor.prepare(None)

            async def call(source, operation, arguments, scope):
                assert (source, operation, scope) == ("schedules", "list_scheduled_tasks", context)
                return await list_tasks(**arguments)

            module.call = call
            discovery = await module.search(context, query="übersicht", limit=1)
            assert [task["id"] for task in discovery["hits"]] == ["digest"]

    asyncio.run(exercise())


def test_automation_history_cursor_continues_after_same_timestamp_and_newer_insert(tmp_path):
    from openagent_core.mcp.servers.events_manager.server import list_event_deliveries
    from openagent_core.mcp.servers.workflow_manager.server import list_workflow_runs

    repository = AutomationRepository(tmp_path / "agent.db")

    async def exercise():
        async with repository.session() as connection:
            await connection.executescript(SCHEMA_SQL)
            await connection.execute(
                "INSERT INTO events (id,name,slug,secret_enc,action_kind,created_at,updated_at) "
                "VALUES ('event','Event','event','fixture','prompt',0,0)"
            )
            await connection.execute(
                "INSERT INTO workflow_tasks (id,name,created_at,updated_at) VALUES ('flow','Workflow',0,0)"
            )
            for identifier, timestamp in (("a", 2), ("b", 2), ("c", 1)):
                await connection.execute(
                    "INSERT INTO event_deliveries (id,event_id,status,started_at) VALUES (?,'event','success',?)",
                    (identifier, timestamp),
                )
                await connection.execute(
                    "INSERT INTO workflow_runs (id,workflow_id,trigger,status,started_at) "
                    "VALUES (?,'flow','manual','success',?)", (identifier, timestamp),
                )
            events = await list_event_deliveries("event", limit=1)
            flows = await list_workflow_runs("flow", limit=1, status="success")
            assert [row["id"] for row in events["results"]] == ["a"]
            assert [row["id"] for row in flows["results"]] == ["a"]
            await connection.execute(
                "INSERT INTO event_deliveries (id,event_id,status,started_at) VALUES ('new','event','success',3)"
            )
            await connection.execute(
                "INSERT INTO workflow_runs (id,workflow_id,trigger,status,started_at) "
                "VALUES ('new','flow','manual','success',3)"
            )
            for page in (
                await list_event_deliveries("event", cursor=events["next_cursor"]),
                await list_workflow_runs("flow", status="success", cursor=flows["next_cursor"]),
            ):
                assert [row["id"] for row in page["results"]] == ["b", "c"]
                assert page["next_cursor"] is None
            with pytest.raises(ValueError, match="Invalid page cursor"):
                await list_workflow_runs("flow", status="failed", cursor=flows["next_cursor"])

    asyncio.run(exercise())
