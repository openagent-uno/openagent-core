"""Opt-in HTTP access to the authenticated user's exact task desktop."""

from contextlib import asynccontextmanager
import re

from aiohttp import web
from openagent_core.capabilities import CapabilityUnavailable
from openagent_core.computer_interaction import ComputerInteraction

_ID = re.compile(r"[^/\\?#\x00]{1,300}")
_TARGET = re.compile(r"[a-f0-9]{64}")
_PRIVATE = {"Cache-Control": "no-store"}


class ComputerViewerGateway:
    def __init__(self, broker, resolve_task, *, prefix="/api/v1/sessions"):
        self.broker = broker
        self.resolve_task = resolve_task
        self.prefix = prefix.rstrip("/")

    def install(self, app, runtime, resolve_context):
        viewer = ComputerInteraction(runtime)
        route = self.prefix + "/{session_id}/computers/{computer_id}"
        catalog = runtime.capabilities

        @asynccontextmanager
        async def assignment(request, *, interactive):
            session, alias = request.match_info["session_id"], request.match_info["computer_id"]
            if not _ID.fullmatch(session) or not _ID.fullmatch(alias) or request.query:
                raise ValueError("Invalid viewer scope")
            action = "computer.viewer.interact" if interactive else "computer.viewer.targets"
            async with resolve_context(request, action, None, session) as context:
                if context.session_id != session:
                    raise PermissionError("Viewer session mismatch")
                await viewer.authorize_access(context, interactive=interactive)
                task = await self.resolve_task(request, context, alias)
                if task is None:
                    raise CapabilityUnavailable("No current viewer assignment")
                yield context, await self.broker.current_lease(context, task)

        async def desktops(bound):
            for item in await viewer.targets(bound):
                descriptor = await catalog.describe_tool(item["tool_ref"], bound)
                ident = self.broker.viewer_target_id(catalog, bound, descriptor)
                yield ident, item

        async def targets(request):
            async with assignment(request, interactive=False) as (context, lease):
                async with self.broker.tools(catalog, context, lease) as bound:
                    items = [
                        {"target_id": ident, "label": item["label"], "live_media": item["live_media"]}
                        async for ident, item in desktops(bound)
                    ]
                    await self.broker.admit_target(bound, lease.host_id, lease.generation)
                    await viewer.authorize_access(context, interactive=False)
                    return web.json_response(
                        {"epoch": lease.epoch, "generation": lease.generation, "targets": items},
                        headers=_PRIVATE,
                    )

        async def interact(request):
            async with assignment(request, interactive=True) as (context, lease):
                body = await request.json()
                fields = {"epoch", "generation", "target_id", "action", "coordinate", "text"}
                if not isinstance(body, dict) or set(body) - fields:
                    raise ValueError("Unexpected desktop request fields")
                versions = (body.get("epoch"), body.get("generation"))
                if any(type(v) is not int or v < 1 for v in versions):
                    raise ValueError("Expected a positive epoch and generation")
                selected = body.get("target_id")
                if not isinstance(selected, str) or not _TARGET.fullmatch(selected):
                    raise ValueError("Expected the exact viewer target")
                if versions != (lease.epoch, lease.generation):
                    return web.json_response(
                        {"error": "stale_computer_target"}, status=409, headers=_PRIVATE
                    )
                async with self.broker.tools(catalog, context, lease) as bound:
                    capture = body.get("action") == "get_screenshot"
                    await self.broker.authorize_viewer_action(bound, lease, capture=capture)
                    async for ident, item in desktops(bound):
                        if ident == selected:
                            wire = {k: v for k, v in body.items() if k in {"action", "coordinate", "text"}}
                            result = await viewer.interact(bound, {"tool_ref": item["tool_ref"], **wire})
                            await self.broker.authorize_viewer_action(bound, lease, capture=capture)
                            return web.json_response(result, headers=_PRIVATE)
                    raise CapabilityUnavailable("The selected desktop is unavailable")

        app.router.add_get(route + "/targets", targets, allow_head=False)
        app.router.add_post(route + "/interact", interact)
