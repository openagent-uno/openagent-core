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


class StreamingDesktop(Desktop):
    def __init__(self):
        super().__init__(definition(metadata={"openagent/liveMedia": "openagent-video/1"}))
        self.streams = 0
        self.waiting = asyncio.Event()
        self.next_packet = asyncio.Event()
        self.closed = asyncio.Event()

    async def stream_tool(self, name, options, context):
        assert name == self.tool.name
        assert options == {"fps": 10}
        assert current_execution_context() == context
        self.streams += 1
        try:
            yield {"seq": 0}
            self.waiting.set()
            await self.next_packet.wait()
            assert current_execution_context() == context
            yield {"seq": 1}
        finally:
            self.closed.set()


async def registered_stream(setup):
    service, runtime, context, _, _ = setup
    desktop = StreamingDesktop()
    runtime.capabilities.register("assigned-display", desktop, desktop, target_label="Workload display")
    target, = await service.targets(context, source_id="assigned-display")
    return target["tool_ref"], desktop


@pytest.mark.asyncio
async def test_live_desktop_requires_the_viewer_and_exact_producer(setup):
    service, _, context, policy, _ = setup
    tool_ref, desktop = await registered_stream(setup)

    for action in ("run.execute", "tool.call", "tool.publish"):
        policy.denied.add(action)
        with pytest.raises(PermissionError):
            await anext(service.stream(context, tool_ref, {"fps": 10}, source_id="assigned-display"))
        policy.denied.clear()
    with pytest.raises(CapabilityUnavailable):
        await anext(service.stream(context, tool_ref, {"fps": 10}, source_id="sandbox"))
    with pytest.raises(PermissionError):
        delayed = replace(context, deferred=True, delegation_id="deferred")
        await anext(service.stream(delayed, tool_ref, {"fps": 10}))
    with pytest.raises(ValueError):
        await anext(service.stream(context, tool_ref, {"environment": {"DISPLAY": ":0"}}))
    assert desktop.streams == 0

    stream = service.stream(context, tool_ref, {"fps": 10}, source_id="assigned-display")
    assert await asyncio.create_task(anext(stream)) == {"seq": 0}
    desktop.next_packet.set()
    assert await asyncio.create_task(anext(stream)) == {"seq": 1}
    await stream.aclose()
    assert desktop.streams == 1 and desktop.closed.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", ["viewer", "producer", "publisher", "registration"])
async def test_waiting_producer_is_closed_after_revocation(setup, monkeypatch, revoked):
    from openagent_core import computer_interaction

    monkeypatch.setattr(computer_interaction, "_STREAM_AUTHORIZATION_INTERVAL_SECONDS", 0.01)
    service, runtime, context, policy, _ = setup
    tool_ref, desktop = await registered_stream(setup)
    stream = service.stream(context, tool_ref, {"fps": 10}, source_id="assigned-display")
    assert await anext(stream) == {"seq": 0}
    packet = asyncio.create_task(anext(stream))
    await asyncio.wait_for(desktop.waiting.wait(), 1)
    if revoked == "registration":
        runtime.capabilities.revoke("assigned-display")
        replacement = StreamingDesktop()
        runtime.capabilities.register("assigned-display", replacement, replacement, target_label="Workload display")
        error = CapabilityUnavailable
    else:
        policy.denied.add({"viewer": "run.execute", "producer": "tool.call", "publisher": "tool.publish"}[revoked])
        error = PermissionError

    with pytest.raises(error):
        await asyncio.wait_for(packet, 1)
    assert desktop.closed.is_set()
    if revoked == "registration":
        assert replacement.streams == 0


@pytest.mark.asyncio
async def test_late_media_is_suppressed_and_disconnect_releases_the_producer(setup):
    service, _, context, policy, _ = setup
    tool_ref, desktop = await registered_stream(setup)
    stream = service.stream(context, tool_ref, {"fps": 10}, source_id="assigned-display")
    assert await anext(stream) == {"seq": 0}
    packet = asyncio.create_task(anext(stream))
    await asyncio.wait_for(desktop.waiting.wait(), 1)
    policy.denied.add("run.execute")
    desktop.next_packet.set()
    with pytest.raises(PermissionError):
        await asyncio.wait_for(packet, 1)
    assert desktop.closed.is_set()

    policy.denied.clear()
    desktop.waiting.clear()
    desktop.closed.clear()
    desktop.next_packet.clear()
    stream = service.stream(context, tool_ref, {"fps": 10}, source_id="assigned-display")
    assert await anext(stream) == {"seq": 0}
    packet = asyncio.create_task(anext(stream))
    await asyncio.wait_for(desktop.waiting.wait(), 1)
    packet.cancel()
    with pytest.raises(asyncio.CancelledError):
        await packet
    assert desktop.closed.is_set()
