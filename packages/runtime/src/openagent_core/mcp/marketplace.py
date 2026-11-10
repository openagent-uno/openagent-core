"""Versioned MCP Registry metadata and installation recipe translation."""
from __future__ import annotations

import asyncio
import json
import re
from urllib.parse import quote

import httpx

from ..capabilities import CapabilityUnavailable

REGISTRY_BASE = "https://registry.modelcontextprotocol.io/v0.1"
_MAX_RESPONSE = 2 << 20

PLACEHOLDER_RE = re.compile(r"\{([A-Za-z0-9_]+)\}")

# The set of runtimes we know how to assemble into argv. Anything else → 422
# at install time so we don't silently emit broken commands.
_KNOWN_RUNTIMES = {"npx", "uvx", "docker", "dnx"}

# Many registry entries omit ``runtimeHint`` and only set ``registryType``
# (the registry the artefact lives in). Map those to the conventional
# runner that pulls + executes from that registry. Mirrors how a human
# developer would translate "npm package X" → ``npx X``.
_REGISTRY_TYPE_TO_RUNTIME = {
    "npm": "npx",
    "pypi": "uvx",
    "oci": "docker",
    "docker": "docker",
    "nuget": "dnx",
}


def _resolve_runtime(pkg: dict) -> str | None:
    """Pick a runtime for a package: explicit hint wins, else infer from registryType."""
    hint = pkg.get("runtimeHint")
    if hint:
        return hint
    rtype = pkg.get("registryType")
    if rtype:
        return _REGISTRY_TYPE_TO_RUNTIME.get(rtype, rtype)
    return None

def _placeholders_in(value: str | None) -> list[str]:
    if not value:
        return []
    return list(dict.fromkeys(PLACEHOLDER_RE.findall(value)))


def _env_field(item: dict) -> dict:
    """Normalise an ``environmentVariables[]`` or ``headers[]`` item."""
    return {
        "name": item.get("name"),
        "isSecret": bool(item.get("isSecret", False)),
        "isRequired": bool(item.get("isRequired", False)),
        "description": item.get("description"),
        "default": item.get("default"),
        # The literal template; placeholders get substituted at install time.
        "value_template": item.get("value"),
    }


def _collect_arg_placeholders(args: list[dict]) -> list[dict]:
    """Walk runtimeArguments/packageArguments and collect placeholder tokens."""
    out: list[dict] = []
    seen: set[str] = set()
    for arg in args or []:
        for token in _placeholders_in(arg.get("value")):
            if token in seen:
                continue
            seen.add(token)
            out.append({
                "token": token,
                "description": arg.get("description"),
            })
    return out


def _synthesise_requirements(server: dict) -> dict:
    """Walk server.json packages[] and remotes[] into a UI-friendly shape."""
    packages_out: list[dict] = []
    for i, pkg in enumerate(server.get("packages") or []):
        runtime = _resolve_runtime(pkg) or "unknown"
        env_required = [_env_field(e) for e in (pkg.get("environmentVariables") or [])]
        placeholders = _collect_arg_placeholders(
            (pkg.get("runtimeArguments") or []) + (pkg.get("packageArguments") or [])
        )
        packages_out.append({
            "index": i,
            "runtime": runtime,
            "registryType": pkg.get("registryType"),
            "identifier": pkg.get("identifier") or pkg.get("name"),
            "version": pkg.get("version"),
            "transport": (pkg.get("transport") or {}).get("type"),
            "env_required": env_required,
            "placeholders": placeholders,
            "supported": runtime in _KNOWN_RUNTIMES,
        })

    remotes_out: list[dict] = []
    for i, remote in enumerate(server.get("remotes") or []):
        header_required = [_env_field(h) for h in (remote.get("headers") or [])]
        # Placeholders can appear in the URL too; surface them.
        placeholders = []
        for token in _placeholders_in(remote.get("url")):
            placeholders.append({"token": token, "description": "URL placeholder"})
        remotes_out.append({
            "index": i,
            "url": remote.get("url"),
            "transport": remote.get("type") or (remote.get("transport") or {}).get("type") or "streamable-http",
            "header_required": header_required,
            "placeholders": placeholders,
        })

    return {"packages": packages_out, "remotes": remotes_out}


def _substitute(template: str | None, values: dict) -> str | None:
    if template is None:
        return None
    return PLACEHOLDER_RE.sub(lambda m: str(values.get(m.group(1), m.group(0))), template)


def _resolve_arg_values(args: list[dict], placeholders: dict) -> list[str]:
    """Turn server.json runtime/package arguments into a flat argv slice.

    Each arg is either positional ({type: "positional", value, ...}) or
    named ({type: "named", name, value, ...}). For named with no value, we
    skip — that's a flag-only switch like ``--verbose`` controlled elsewhere.
    """
    out: list[str] = []
    for arg in args or []:
        atype = arg.get("type") or "positional"
        value = _substitute(arg.get("value"), placeholders)
        if atype == "named":
            name = arg.get("name")
            if name:
                out.append(name)
            if value is not None and value != "":
                out.append(value)
        else:  # positional
            if value is not None:
                out.append(value)
    return out


def _build_install_payload(
    server: dict,
    choice: dict,
    env_in: dict,
    headers_in: dict,
    placeholders: dict,
) -> tuple[dict, str | None]:
    """Map server.json + user inputs → kwargs for ``db.upsert_mcp``.

    Returns (kwargs, error). On unsupported runtime / missing required value,
    returns ({}, "<error message>") so the caller can 400/422.
    """
    kind = choice.get("kind")
    idx = choice.get("index", 0)

    # ── Package (stdio) ──
    if kind == "package":
        packages = server.get("packages") or []
        if not 0 <= idx < len(packages):
            return {}, f"package index {idx} out of range"
        pkg = packages[idx]
        runtime = _resolve_runtime(pkg)
        identifier = pkg.get("identifier") or pkg.get("name")
        version = pkg.get("version")
        if runtime not in _KNOWN_RUNTIMES:
            return {}, f"unsupported runtime {runtime!r} (no install recipe)"
        if not identifier:
            return {}, "package is missing 'identifier'"

        runtime_args = _resolve_arg_values(pkg.get("runtimeArguments") or [], placeholders)
        package_args = _resolve_arg_values(pkg.get("packageArguments") or [], placeholders)

        if runtime == "npx":
            command = ["npx"]
            spec = f"{identifier}@{version}" if version else identifier
            args = ["-y", *runtime_args, spec, *package_args]
        elif runtime == "uvx":
            command = ["uvx"]
            spec = f"{identifier}=={version}" if version else identifier
            args = [*runtime_args, spec, *package_args]
        elif runtime == "dnx":
            command = ["dnx"]
            args = [*runtime_args, identifier, *package_args]
        else:  # docker / oci
            command = ["docker"]
            ref = f"{identifier}:{version}" if version else identifier
            args = ["run", "-i", "--rm", *runtime_args, ref, *package_args]

        # Resolve env: defaults + caller overrides; substitute placeholders.
        env: dict[str, str] = {}
        missing: list[str] = []
        for entry in pkg.get("environmentVariables") or []:
            name = entry.get("name")
            if not name:
                continue
            supplied = env_in.get(name)
            if supplied is None or supplied == "":
                tmpl = _substitute(entry.get("value"), placeholders)
                if tmpl not in (None, ""):
                    env[name] = tmpl
                elif entry.get("default") is not None:
                    env[name] = str(entry["default"])
                elif entry.get("isRequired"):
                    missing.append(name)
            else:
                env[name] = str(supplied)
        if missing:
            return {}, f"missing required env vars: {', '.join(missing)}"

        return {
            "command": command,
            "args": args,
            "url": None,
            "env": env,
            "headers": {},
        }, None

    # ── Remote (HTTP/SSE) ──
    if kind == "remote":
        remotes = server.get("remotes") or []
        if not 0 <= idx < len(remotes):
            return {}, f"remote index {idx} out of range"
        remote = remotes[idx]
        url = _substitute(remote.get("url"), placeholders)
        if not url:
            return {}, "remote is missing 'url'"

        headers: dict[str, str] = {}
        missing: list[str] = []
        for entry in remote.get("headers") or []:
            name = entry.get("name")
            if not name:
                continue
            supplied = headers_in.get(name)
            if supplied is None or supplied == "":
                tmpl = _substitute(entry.get("value"), placeholders)
                if tmpl not in (None, ""):
                    headers[name] = tmpl
                elif entry.get("default") is not None:
                    headers[name] = str(entry["default"])
                elif entry.get("isRequired"):
                    missing.append(name)
            else:
                headers[name] = str(supplied)
        if missing:
            return {}, f"missing required headers: {', '.join(missing)}"

        return {
            "command": None,
            "args": [],
            "url": url,
            "env": {},
            "headers": headers,
        }, None

    return {}, f"unknown choice.kind {kind!r} (expected 'package' or 'remote')"


def _default_install_name(registry_name: str) -> str:
    """``io.github.foo/postgres`` → ``postgres``; fallback: full registry name."""
    if not registry_name:
        return "mcp-marketplace"
    tail = registry_name.rsplit("/", 1)[-1]
    # Normalise to a slug — names go straight into the mcps PRIMARY KEY.
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", tail).strip("-").lower()
    return slug or "mcp-marketplace"



class Marketplace:
    """MCP Registry v0.1. Only host-selected, HTTPS registries are supported."""

    def __init__(self, client=None):
        self._client = client

    async def _request(self, path, params=None):
        if self._client is not None:
            return await self._read(self._client, path, params)
        async with httpx.AsyncClient(timeout=10, follow_redirects=False, trust_env=False,
                                     headers={"User-Agent": "OpenAgent-Marketplace/1.0"}) as client:
            return await self._read(client, path, params)

    @staticmethod
    async def _read(client, path, params):
        try:
            async with asyncio.timeout(12):
                async with client.stream("GET", REGISTRY_BASE + path, params=params) as response:
                    if response.status_code == 404:
                        raise CapabilityUnavailable("MCP Registry entry was not found")
                    response.raise_for_status()
                    body = bytearray()
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        if len(body) + len(chunk) > _MAX_RESPONSE:
                            raise CapabilityUnavailable("MCP Registry response exceeds its size limit")
                        body.extend(chunk)
                    data = json.loads(body)
        except (TimeoutError, httpx.HTTPError, ValueError) as exc:
            raise CapabilityUnavailable("MCP Registry is temporarily unavailable") from exc
        if not isinstance(data, dict):
            raise CapabilityUnavailable("Invalid MCP Registry response")
        return data

    async def search(self, query, cursor=""):
        query = query.strip() if isinstance(query, str) else None
        if (query is None or not 3 <= len(query) <= 120 or not isinstance(cursor, str)
                or len(cursor) > 2048):
            raise ValueError("Search with 3–120 characters and a valid registry cursor")
        params = {"search": query, "version": "latest", "limit": 12}
        if cursor:
            params["cursor"] = cursor
        body = await self._request("/servers", params)
        entries = body.get("servers") or []
        if not isinstance(entries, list) or len(entries) > 12:
            raise CapabilityUnavailable("Invalid MCP Registry result page")
        return {"servers": [_card(entry["server"]) for entry in entries],
                "next_cursor": (body.get("metadata") or {}).get("nextCursor")}

    async def server(self, name, version):
        _identity(name, version)
        path = f"/servers/{quote(name, safe='')}/versions/{quote(version, safe='')}"
        server = (await self._request(path)).get("server")
        if (not isinstance(server, dict) or server.get("name") != name
                or (version != "latest" and server.get("version") != version)
                or server.get("status") == "deleted"):
            raise CapabilityUnavailable("MCP Registry returned a different or retired entry")
        return server


def _identity(name, version):
    if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,119}/[A-Za-z0-9][A-Za-z0-9_.-]{0,119}", name)
            or not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,95}", version)):
        raise ValueError("Supply a registry name and version")


def registry_entry_url(name, version):
    """A stable, credential-free URL suitable for a conversation link."""
    _identity(name, version)
    return f"{REGISTRY_BASE}/servers/{quote(name, safe='')}/versions/{quote(version, safe='')}"


def _card(server):
    name, version = server.get("name"), server.get("version")
    return {
        "name": name,
        "version": version,
        "title": str(server.get("title") or name)[:120],
        "description": str(server.get("description") or "")[:500],
        "status": server.get("status", "active"),
        "registry_url": registry_entry_url(name, version),
    }
