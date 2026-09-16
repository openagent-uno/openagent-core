"""Uniform catalog management tools backed by the host's public service.

There is no standalone SQLite mutation path. Hosts with fixed catalogs do not
supply a management service; calling these tools then fails closed.
"""
from __future__ import annotations
from typing import Any
from openagent_core.runtime import current_runtime, current_execution_context


def _service():
    runtime, context = current_runtime(), current_execution_context()
    if runtime is None or context is None or runtime.services.catalog_management is None:
        raise PermissionError('This host does not expose dynamic catalog management')
    return runtime.services.catalog_management, context


async def list_mcps(enabled_only: bool = False) -> list[dict[str, Any]]:
    """List sources and their host-owned mutability policy."""
    service, context = _service()
    rows = await service.list(context)
    return [row for row in rows if row.get('enabled')] if enabled_only else rows


async def get_mcp(name: str) -> dict[str, Any]:
    """Read one source's configuration using the current verified identity."""
    service, context = _service()
    return await service.get(context, name)


async def add_custom_mcp(name: str, command: list[str] | None = None,
                         args: list[str] | None = None, url: str | None = None,
                         env: dict[str, str] | None = None, enabled: bool = True) -> dict[str, Any]:
    """Install a user-managed source if this host and current principal permit it."""
    service, context = _service()
    return await service.create(context, name, command=command, args=args or [],
                                url=url, env=env or {}, enabled=enabled)


async def update_mcp(name: str, command: list[str] | None = None,
                     args: list[str] | None = None, url: str | None = None,
                     env: dict[str, str] | None = None, enabled: bool | None = None) -> dict[str, Any]:
    """Change allowed configuration fields; product ownership cannot be changed."""
    service, context = _service()
    values = {key: value for key, value in dict(command=command,args=args,url=url,env=env,enabled=enabled).items() if value is not None}
    return await service.update(context, name, **values)


async def enable_mcp(name: str) -> dict[str, Any]:
    """Enable one source through its host policy."""
    service, context = _service()
    return await service.set_enabled(context, name, True)


async def disable_mcp(name: str) -> dict[str, Any]:
    """Disable one source through its host policy."""
    service, context = _service()
    return await service.set_enabled(context, name, False)


async def remove_mcp(name: str) -> dict[str, Any]:
    """Remove a user source; managed product sources cannot be removed."""
    service, context = _service()
    return await service.delete(context, name)


async def list_builtin_mcps() -> list[dict[str, Any]]:
    """List the product-managed sources selected by this host."""
    service, context = _service()
    rows = await service.list(context)
    return [dict(builtin_name=row.get('builtin_name') or row['name'], configured=True,
                 managed=True, enabled=row.get('enabled',False)) for row in rows if row.get('managed')]


def build_runtime_toolkit():
    from openagent_core.mcp._runtime import Toolkit
    return Toolkit(name='mcp-manager', tools=[list_mcps,get_mcp,add_custom_mcp,update_mcp,
        enable_mcp,disable_mcp,remove_mcp,list_builtin_mcps])


def main():
    raise RuntimeError('Catalog management must be hosted by an authorized Runtime')


if __name__ == '__main__':
    main()
