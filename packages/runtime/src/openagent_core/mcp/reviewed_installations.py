"""Allow an admitted agent to activate exact host-reviewed MCP installations."""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping, Protocol

from ..capabilities import CapabilityUnavailable, ToolDefinition
from ..contracts import ExecutionContext, ResourceRef
from ..runtime import current_execution_context, current_run_id, current_runtime

_ID = re.compile(r"[a-z0-9][a-z0-9_.-]{0,95}")
_REVISION = re.compile(r"[a-f0-9]{64}")
_CATALOG = ToolDefinition(
    "list_reviewed_mcps",
    "List MCP integrations that the current run may install for this agent.",
    {"type": "object", "properties": {}, "additionalProperties": False},
)
_INSTALL = ToolDefinition(
    "install_reviewed_mcp",
    "Activate a reviewed MCP integration for this agent. Use the exact ID and revision "
    "from list_reviewed_mcps. Rediscover its tools on the next turn.",
    {
        "type": "object",
        "properties": {
            "id": {"type": "string", "pattern": "^" + _ID.pattern + "$"},
            "revision": {"type": "string", "pattern": "^" + _REVISION.pattern + "$"},
        },
        "required": ["id", "revision"],
        "additionalProperties": False,
    },
)


@dataclass(frozen=True, slots=True)
class ReviewedMCP:
    id: str
    revision: str
    title: str
    description: str

    def __post_init__(self):
        if (not _ID.fullmatch(self.id) or not _REVISION.fullmatch(self.revision)
                or not 1 <= len(self.title) <= 120 or not 1 <= len(self.description) <= 600
                or "\x00" in self.title or "\x00" in self.description):
            raise ValueError("Invalid reviewed MCP offer")


class ReviewedMCPInstaller(Protocol):
    """Host-owned inventory and activation of exact immutable configurations.

    List only offers eligible for this agent and the current execution audience.
    Install must revalidate the current authority and exact reviewed revision
    at the mutation boundary. It is idempotent for a tenant, agent, offer and
    revision and must fail closed on a conflicting existing configuration.
    Never return credentials, launch arguments or private destination details.
    """

    async def offers(self, context: ExecutionContext) -> tuple[ReviewedMCP, ...]: ...
    async def install(self, context: ExecutionContext, offer: ReviewedMCP) -> None: ...


class ReviewedMCPInstallationCapability:
    """Optional source for managed hosts, independent of raw MCP administration."""

    def __init__(self, installer: ReviewedMCPInstaller):
        self._installer = installer

    async def discover(self, context: ExecutionContext) -> tuple[ToolDefinition, ...]:
        runtime = current_runtime()
        if runtime is None or current_execution_context() != context or not current_run_id():
            return ()
        try:
            await self._authorize(runtime, context, "catalog.list", "reviewed-mcps")
        except PermissionError:
            return ()
        return (_CATALOG, _INSTALL)

    async def call_tool(
        self, name: str, arguments: Mapping[str, Any], context: ExecutionContext
    ) -> dict[str, Any]:
        runtime = current_runtime()
        if runtime is None or current_execution_context() != context or not current_run_id():
            raise PermissionError("MCP installation requires an authenticated run")
        if name == _CATALOG.name:
            if arguments:
                raise ValueError("Reviewed catalog takes no arguments")
            return {"offers": [
                {"id": offer.id, "revision": offer.revision, "title": offer.title,
                 "description": offer.description}
                for offer in await self._offers(runtime, context)
            ]}
        if name != _INSTALL.name:
            raise CapabilityUnavailable("Unknown reviewed MCP installation tool")
        if (set(arguments) != {"id", "revision"}
                or not isinstance(arguments["id"], str) or not _ID.fullmatch(arguments["id"])
                or not isinstance(arguments["revision"], str) or not _REVISION.fullmatch(arguments["revision"])):
            raise ValueError("Installation requires only the reviewed ID and revision")

        from ..memory.vault.privacy import require_unprotected_execution

        await require_unprotected_execution("Reviewed MCP installation")
        offers = await self._offers(runtime, context)
        offer = next((item for item in offers if item.id == arguments["id"]
                      and item.revision == arguments["revision"]), None)
        if offer is None:
            raise CapabilityUnavailable("This reviewed MCP revision is unavailable")
        await self._authorize(runtime, context, "catalog.install", offer.id)
        await self._installer.install(context, offer)
        return {"id": offer.id, "revision": offer.revision, "installed": True}

    @staticmethod
    async def _authorize(runtime, context, action, resource_id):
        await runtime.authorize(context, action, ResourceRef("mcp", context.tenant_id, resource_id),
                                audience=context.audience)

    async def _offers(self, runtime, context):
        await self._authorize(runtime, context, "catalog.list", "reviewed-mcps")
        offers = await self._installer.offers(context)
        await self._authorize(runtime, context, "catalog.list", "reviewed-mcps")
        if (len(offers) > 128 or any(not isinstance(item, ReviewedMCP) for item in offers)
                or len({item.id for item in offers}) != len(offers)):
            raise ValueError("Invalid reviewed MCP inventory")
        return offers
