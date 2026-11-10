import asyncio
import json
from types import SimpleNamespace

from openagent_core.automation import AutomationRepository, build_automation_toolkit
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.core.tool_output import DEFAULT_MAX_TOOL_RESULT_CHARS
from openagent_core.memory.db import SCHEMA_SQL
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

        runtime = SimpleNamespace(services=SimpleNamespace(automation_management=Management()))
        list_tasks = toolkit.async_functions["list_scheduled_tasks"].entrypoint
        get_task = toolkit.async_functions["get_scheduled_task"].entrypoint
        with execution_scope(runtime, context, "run"):
            catalog = await list_tasks()
            assert len(catalog) == len(definitions)
            assert "digest" in {task["id"] for task in catalog}
            assert all("prompt" not in task for task in catalog)
            assert len(json.dumps(catalog)) < DEFAULT_MAX_TOOL_RESULT_CHARS
            assert len(json.dumps(definitions)) > DEFAULT_MAX_TOOL_RESULT_CHARS

            matches = await list_tasks(query="  ÜBERSICHT digest  ")
            assert [(task["id"], task["enabled"]) for task in matches] == [
                ("digest", True), ("paused", False),
            ]
            active = await list_tasks(True, query="übersicht")
            assert [task["id"] for task in active] == ["digest"]
            assert await list_tasks(query="missing") == []
            assert (await get_task("digest"))["prompt"] == prompt
            detailed = await list_tasks(query="übersicht", enabled_only=True, include_prompt=True)
            assert detailed == [await get_task("digest")]

    asyncio.run(exercise())
