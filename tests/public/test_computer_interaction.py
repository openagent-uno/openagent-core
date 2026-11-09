"""A desktop viewer must use one live, authorized capability, not an arbitrary tool."""
import asyncio
from dataclasses import replace

import pytest

from openagent_core import ExecutionContext, PrincipalRef, Runtime, RuntimeServices, RuntimeSettings
from openagent_core.capabilities import CapabilityUnavailable, ToolDefinition
from openagent_core.computer_interaction import ComputerInteraction
from openagent_core.runtime import current_execution_context, current_run_id


class Policy:
    def __init__(self):
        self.denied = set()

    async def authorize(self, context, action, resource, *, audience=()):
        return (
            action not in self.denied
            and resource.tenant_id == context.tenant_id
            and tuple(audience) == context.audience
        )


def definition(*, required=("action",), metadata=None, name="computer"):
    return ToolDefinition(
        name,
        "Test desktop",
        {"type": "object", "required": list(required), "properties": {"action": {"type": "string"}}},
        metadata=metadata or {},
    )


_FRAME = {"content": [
    {"type": "text", "text": '{"image_width":1,"image_height":1}'},
    {"type": "image", "mimeType": "image/png", "data":
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ipxkAAAAASUVORK5CYII="},
], "isError": False}


class Desktop:
    def __init__(self, tool=None):
        self.tool = tool or definition()
        self.calls = []
        self.discoveries = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def discover(self, context):
        self.discoveries += 1
        return (self.tool,)

    async def call_tool(self, name, arguments, context):
        assert current_run_id() is None
        assert current_execution_context() == context
        self.calls.append((name, arguments))
        self.started.set()
        await self.release.wait()
        return _FRAME if arguments["action"] == "get_screenshot" else {"content": [], "isError": False}


@pytest.fixture
def setup(tmp_path):
    user = PrincipalRef("host", "tenant", "human", "user")
    context = ExecutionContext(user, user, user, "conversation", "worker", (user,))
    policy = Policy()
    runtime = Runtime(RuntimeSettings("worker", tmp_path), RuntimeServices(object(), object(), policy))
    desktops = {
        "sandbox": Desktop(definition(name="computer_control_computer")),
        "registered-device": Desktop(definition(metadata={"openagent/liveMedia": "video/1"})),
        "browser": Desktop(definition(required=("action", "tabId"))),
        "model-only": Desktop(definition(metadata={"ui": {"visibility": ["model"]}})),
    }
    for source, desktop in desktops.items():
        runtime.capabilities.register(source, desktop, desktop, target_label=source)
    return ComputerInteraction(runtime), runtime, context, policy, desktops


def test_exact_source_filter_and_app_visibility(setup):
    service, runtime, context, _, desktops = setup

    async def check():
        targets = {target["label"]: target for target in await service.targets(context)}
        assert set(targets) == {"sandbox", "registered-device"}
        assert targets["registered-device"]["live_media"] == "video/1"
        assert await service.targets(context, source_id="sandbox") == [targets["sandbox"]]
        assert {source: desktop.discoveries for source, desktop in desktops.items()} == {
            "sandbox": 2, "registered-device": 1, "browser": 1, "model-only": 1,
        }
        screenshot = {"tool_ref": targets["sandbox"]["tool_ref"], "action": "get_screenshot"}
        result = await service.interact(context, screenshot, source_id="sandbox")
        assert result == _FRAME
        assert desktops["sandbox"].calls == [("computer_control_computer", {"action": "get_screenshot"})]
        for tool in await runtime.capabilities.discover(context):
            if tool.source_id != "sandbox":
                other = {**screenshot, "tool_ref": tool.tool_ref}
                with pytest.raises(CapabilityUnavailable):
                    await service.interact(context, other, source_id="sandbox")
                if tool.source_id in {"browser", "model-only"}:
                    with pytest.raises(CapabilityUnavailable):
                        await service.interact(context, other)
        assert all(not desktop.calls for source, desktop in desktops.items() if source != "sandbox")

    asyncio.run(check())


def test_permissions_and_revoked_reference_fail_closed(setup):
    service, runtime, context, policy, desktops = setup

    async def check():
        target, = await service.targets(context, source_id="sandbox")
        input_event = {"tool_ref": target["tool_ref"], "action": "left_click", "coordinate": [15, 21]}
        policy.denied.add("run.execute")
        with pytest.raises(PermissionError):
            await service.interact(context, input_event, source_id="sandbox")
        policy.denied.clear()
        policy.denied.add("run.read")
        with pytest.raises(PermissionError):
            await service.targets(context, source_id="sandbox")
        policy.denied.clear()
        with pytest.raises(PermissionError):
            await service.targets(replace(context, deferred=True, delegation_id="scheduled"))
        with pytest.raises(PermissionError):
            await service.targets(replace(context, agent_id="other-agent"))
        runtime.capabilities.revoke("sandbox")
        replacement = Desktop()
        runtime.capabilities.register("sandbox", replacement, replacement, target_label="sandbox")
        with pytest.raises(CapabilityUnavailable):
            await service.interact(context, input_event, source_id="sandbox")
        assert not desktops["sandbox"].calls and not replacement.calls

    asyncio.run(check())


def test_result_is_discarded_if_session_access_changes_during_call(setup):
    service, _, context, policy, desktops = setup

    async def check():
        target, = await service.targets(context, source_id="sandbox")
        desktop = desktops["sandbox"]
        desktop.release.clear()
        operation = asyncio.create_task(service.interact(context, {
            "tool_ref": target["tool_ref"], "action": "type", "text": "À l’écran",
        }, source_id="sandbox"))
        await asyncio.wait_for(desktop.started.wait(), 1)
        policy.denied.add("run.execute")
        desktop.release.set()
        with pytest.raises(PermissionError):
            await operation
        assert desktop.calls == [("computer_control_computer", {"action": "type", "text": "À l’écran"})]

    asyncio.run(check())


@pytest.mark.parametrize("input_event", [
    {"action": "start_screen_recording"},
    {"action": ["type"], "text": "text"},
    {"action": "left_click", "coordinate": [True, 7]},
    {"action": "left_click", "coordinate": [5, 33000]},
    {"action": "scroll", "coordinate": [5, 5], "text": "right:3001"},
    {"action": "type", "text": ""},
    {"action": "get_screenshot", "source_id": "registered-device"},
])
def test_invalid_or_caller_selected_inputs_never_reach_executor(setup, input_event):
    service, _, context, _, desktops = setup

    async def check():
        target, = await service.targets(context, source_id="sandbox")
        with pytest.raises(ValueError):
            await service.interact(context, {"tool_ref": target["tool_ref"], **input_event}, source_id="sandbox")
        assert not desktops["sandbox"].calls

    asyncio.run(check())
