"""Authenticated discovery and installation of versioned MCP Registry entries."""
from __future__ import annotations

import hashlib
from typing import Protocol

from ..capabilities import CapabilityUnavailable, ToolDefinition
from ..contracts import ExecutionContext, ResourceRef, canonical_json
from ..runtime import current_execution_context, current_run_id, current_runtime
from .marketplace import Marketplace, _build_install_payload, _card, _placeholders_in, _synthesise_requirements


def _tool(name, description, properties, required):
    return ToolDefinition(name, description, {
        "type": "object", "properties": properties, "required": required,
        "additionalProperties": False,
    })


_TEXT = {"type": "string"}
_ENTRY = {"name": _TEXT, "version": _TEXT}
_SEARCH = _tool("search_mcp_marketplace", "Search for official and third-party MCP integrations by name.",
                {"query": _TEXT, "cursor": _TEXT}, ["query"])
_INSPECT = _tool("inspect_mcp_marketplace_entry", "Inspect install options and configuration for a marketplace integration.",
                 _ENTRY, ["name", "version"])
_INSTALL = _tool("install_mcp_marketplace_entry", "Install a marketplace integration using the exact inspected revision. "
                 "Supply only non-secret settings. The returned registry URL can be shared as a link in the conversation.",
                 {**_ENTRY, "revision": _TEXT, "kind": {"enum": ["remote", "package"]},
                  "index": {"type": "integer"}, "settings": {"type": "object",
                                                           "additionalProperties": _TEXT}},
                 ["name", "version", "revision", "kind", "index"])


class MarketplaceInstaller(Protocol):
    """A host-owned destination for exact registry recipes.

    A host exposes only transport kinds it can isolate. Installation must
    recheck authorization, bind the result to the current agent and handle
    retries idempotently. The model never selects credentials, executable
    paths, raw environment or network headers.
    """
    def supports(self, kind: str, recipe: dict) -> bool: ...
    async def install(self, context: ExecutionContext, entry: dict, recipe: dict) -> str: ...


class MarketplaceCapability:
    def __init__(self, registry: Marketplace, installer: MarketplaceInstaller | None = None):
        self._registry = registry
        self._installer = installer

    async def discover(self, context):
        if current_execution_context() != context or not current_run_id() or current_runtime() is None:
            return ()
        try:
            await self._authorize(context, "catalog.list", "mcp-marketplace")
        except PermissionError:
            return ()
        if self._installer is not None:
            try:
                await self._authorize(context, "catalog.install", "mcp-marketplace")
            except PermissionError:
                pass
            else:
                return (_SEARCH, _INSPECT, _INSTALL)
        return (_SEARCH, _INSPECT)

    @staticmethod
    async def _authorize(context, action, resource_id):
        await current_runtime().authorize(context, action, ResourceRef("mcp", context.tenant_id, resource_id),
                                          audience=context.audience)

    async def call_tool(self, name, arguments, context):
        if current_execution_context() != context or not current_run_id() or current_runtime() is None:
            raise PermissionError("MCP marketplace access requires an authenticated run")
        await self._authorize(context, "catalog.list", "mcp-marketplace")
        if name == _SEARCH.name:
            if set(arguments) - {"query", "cursor"}:
                raise ValueError("Invalid marketplace search arguments")
            result = await self._registry.search(arguments.get("query"), arguments.get("cursor", ""))
            await self._authorize(context, "catalog.list", "mcp-marketplace")
            return result
        if name not in {_INSPECT.name, _INSTALL.name}:
            raise CapabilityUnavailable("Unknown marketplace tool")
        install = name == _INSTALL.name
        allowed = {"name", "version", "revision", "kind", "index", "settings"} if install else {"name", "version"}
        if set(arguments) - allowed or "name" not in arguments or "version" not in arguments:
            raise ValueError("Invalid marketplace entry arguments")
        if install:
            from ..memory.vault.privacy import require_unprotected_execution
            await require_unprotected_execution("MCP marketplace installation")
            if self._installer is None or arguments["version"] == "latest":
                raise CapabilityUnavailable("Choose an exact inspected version on an installation-enabled host")
        server = await self._registry.server(arguments["name"], arguments["version"])
        entry = _card(server)
        revision = hashlib.sha256(canonical_json(server).encode()).hexdigest()
        requirements = _synthesise_requirements(server)
        await self._authorize(context, "catalog.list", "mcp-marketplace")
        if not install:
            for key, kind, fields in (("remotes", "remote", "header_required"),
                                      ("packages", "package", "env_required")):
                for choice in requirements[key]:
                    choice["host_supported"] = self._installer is not None and self._installer.supports(kind, choice)
                    choice[fields] = [{k: v for k, v in item.items() if k in ("name", "isSecret", "isRequired")}
                                      for item in choice[fields]]
            return {**entry, "revision": revision, "options": requirements}
        if arguments.get("revision") != revision:
            raise CapabilityUnavailable("Registry metadata changed. Inspect this version again.")
        kind, index = arguments.get("kind"), arguments.get("index")
        if kind not in {"remote", "package"} or type(index) is not int:
            raise ValueError("Choose a marketplace transport and index")
        choices = requirements["remotes" if kind == "remote" else "packages"]
        if not 0 <= index < len(choices) or not self._installer.supports(kind, choices[index]):
            raise CapabilityUnavailable("This host does not support the selected install option")
        settings = arguments.get("settings", {})
        fields = choices[index]["header_required" if kind == "remote" else "env_required"]
        allowed_settings = {item["token"] for item in choices[index]["placeholders"]}
        for field in fields:
            if field["isSecret"]:
                allowed_settings -= set(_placeholders_in(field["value_template"]))
                if field["isRequired"]:
                    raise CapabilityUnavailable("This integration requires a credential supplied through the host")
            elif kind == "package":
                allowed_settings.add(field["name"])
        if (not isinstance(settings, dict) or set(settings) - allowed_settings or len(settings) > 24
                or any(not isinstance(value, str) or len(value) > 256 or any(ord(c) < 32 for c in value)
                       for value in settings.values())):
            raise ValueError("Supply only the declared non-secret configuration fields")
        if any(item["isSecret"] for item in fields):
            raise CapabilityUnavailable("This integration requires a host credential binding")
        recipe, error = _build_install_payload(server, {"kind": kind, "index": index}, settings, {}, settings)
        if error or any(_placeholders_in(value) for value in [recipe.get("url"), *recipe.get("args", ()),
                                                     *recipe.get("headers", {}).values(), *recipe.get("env", {}).values()]):
            raise CapabilityUnavailable("The integration configuration is incomplete")
        await self._authorize(context, "catalog.install", "mcp-marketplace")
        installed = await self._installer.install(context, entry, recipe)
        return {**entry, "installation": installed, "available_next_turn": True}

