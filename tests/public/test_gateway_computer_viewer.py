"""A viewer handle can move between authenticated HTTP workers within one lease."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
import time

from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy.ext.asyncio import create_async_engine

from openagent_core import ExecutionContext, PrincipalRef, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.computer_tasks import ComputerTask, TaskComputerBroker
from openagent_gateway import ComputerViewerGateway, create_app
from test_computer_tasks import Machines, Policy


class DesktopFleet(Machines):
    digest = "a" * 64

    async def catalog(self, context, selectors):
        data = await super().catalog(context, selectors)
        for row in data["servers"]:
            row["server"] = "desktop"
            row["catalog_digest"] = self.digest
            row["tools"] = [
                {
                    "name": "computer",
                    "input_schema": {
                        "type": "object",
                        "required": ["action"],
                        "properties": {
                            "action": {
                                "type": "string",
                                "enum": ["get_screenshot", "mouse_move", "left_click", "key", "type"],
                            }
                        },
                    },
                }
            ]
        return data


def test_authenticated_gateway_workers_select_one_current_task_desktop(tmp_path):
    async def run():
        async with AsyncExitStack() as stack:
            engine = create_async_engine("sqlite+aiosqlite:///" + str(tmp_path / "control.db"))
            stack.push_async_callback(engine.dispose)
            fleet = DesktopFleet(time.time)

            class ViewerPolicy(Policy):
                denied = set()

                async def authorize(self, context, action, resource, *, audience=()):
                    return action not in self.denied and await super().authorize(
                        context, action, resource, audience=audience
                    )

            policy = ViewerPolicy()
            broker = TaskComputerBroker(engine, policy, fleet, fleet)
            await broker.initialize()
            p = PrincipalRef("host", "tenant", "user")
            owner = ExecutionContext(p, p, p, "chat", "agent", (p,))
            tasks = {name: ComputerTask("project", name, "base") for name in ("one", "two")}
            for task in tasks.values():
                await broker.provision(owner, task)
                await broker.attach(owner, task)

            @asynccontextmanager
            async def identity(request, action, run_id, session):
                if request.headers.get("Authorization") != "Bearer fixture" or session != owner.session_id:
                    raise PermissionError("Unknown session")
                yield (
                    replace(owner, deferred=True, delegation_id="job")
                    if request.headers.get("X-Test-Job")
                    else owner
                )

            async def resolve(request, context, alias):
                return tasks[alias]

            async def client(enabled):
                runtime = Runtime(
                    RuntimeSettings("agent"), RuntimeServices(object(), None, policy, owns_store=False)
                )
                options = {"computer_viewer": ComputerViewerGateway(broker, resolve)} if enabled else {}
                app = create_app(runtime, identity, manage_lifecycle=False, **options)
                http = TestClient(TestServer(app), headers={"Authorization": "Bearer fixture"})
                await http.start_server()
                stack.push_async_callback(http.close)
                return http

            first, second, disabled = await asyncio.gather(client(True), client(True), client(False))
            endpoint = "/api/v1/sessions/chat/computers/"
            path = endpoint + "one"
            assert (await disabled.get(path + "/targets")).status == 404
            assert (await first.get(path + "/targets", headers={"X-Test-Job": "yes"})).status == 403
            async with first.get(path + "/targets") as response:
                assert response.status == 200 and response.headers["Cache-Control"] == "no-store"
                selection = await response.json()
            assert not broker._mounts
            handle = {
                "epoch": selection["epoch"],
                "generation": selection["generation"],
                "target_id": selection["targets"][0]["target_id"],
                "action": "type",
                "text": "fixture",
            }
            async with second.post(path + "/interact", json=handle) as response:
                assert response.status == 200 and await response.json() == "one"
            assert (await second.post(endpoint + "two/interact", json=handle)).status == 404
            assert (await second.post(path + "/interact", json={**handle, "generation": 0})).status == 400
            assert (
                await second.post(path + "/interact", json={**handle, "epoch": handle["epoch"] + 1})
            ).status == 409
            assert (
                await second.post(path + "/interact", json={**handle, "host_id": "other"})
            ).status == 400
            fleet.digest = "b" * 64
            assert (await second.post(path + "/interact", json=handle)).status == 404
            assert len(fleet.calls) == 1
            fleet.digest = "a" * 64
            policy.denied.add("computer.viewer.input")
            assert (await second.post(path + "/interact", json=handle)).status == 403
            assert len(fleet.calls) == 1
            assert (
                await second.post(
                    path + "/interact", json={**handle, "action": "get_screenshot", "text": None}
                )
            ).status == 200
            policy.denied.clear()
            fleet.digest = "a" * 64

            async def revoke():
                await broker.revoke(owner, tasks["one"])

            fleet.before_return = revoke
            response = await second.post(path + "/interact", json=handle)
            assert response.status == 404 and await response.json() == {"error": "not_found"}
            assert (await first.get(path + "/targets")).status == 404
            assert len(fleet.calls) == 3
            assert not broker._mounts

    asyncio.run(run())
