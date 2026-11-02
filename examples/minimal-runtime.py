"""Generic, local-only kernel wiring example; Python 3.11+."""
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from openagent_core import (
    ExecutionContext, PrincipalRef, RunRequest, Runtime, RuntimeServices, RuntimeSettings,
)
from openagent_storage_sqlite import SqliteRuntimeStore


class DemoPolicy:
    def __init__(self, principal):
        self.principal = principal

    async def authorize(self, context, action, resource, *, audience=()):
        return (
            context.author == context.authority == context.initiator == self.principal
            and context.audience == (self.principal,)
            and resource.tenant_id == self.principal.tenant_id
            and all(member == self.principal for member in audience)
        )


class EchoExecutor:
    async def execute(self, request, context, runtime):
        return request.input


async def main():
    with TemporaryDirectory(prefix="openagent-generic-demo-") as directory:
        workspace = Path(directory).resolve()
        principal = PrincipalRef("demo-authority", "tenant-a", "user-a")
        context = ExecutionContext(
            author=principal, initiator=principal, authority=principal,
            session_id="session-a", agent_id="agent-a", audience=(principal,),
        )
        runtime = Runtime(
            RuntimeSettings("agent-a", workspace),
            RuntimeServices(
                SqliteRuntimeStore(workspace / "runtime.sqlite3"),
                EchoExecutor(), DemoPolicy(principal),
            ),
        )
        await runtime.start()
        try:
            request = RunRequest("run-a", "session-a", "request-a", "hello from a generic runtime")
            await runtime.submit(request, context)
            result = await runtime.wait(request.run_id, context)
            if result.status != "success":
                raise RuntimeError(f"Demo run ended as {result.status}")
            print(result.output)
        finally:
            await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
