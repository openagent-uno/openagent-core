"""Network boundary for an MCP URL selected by an agent."""
from __future__ import annotations

import asyncio
import ipaddress
import re
import socket

import httpx


_DNS_NAME = re.compile(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{0,62}")


def validate_public_https(endpoint: str) -> httpx.URL:
    if not isinstance(endpoint, str) or not 1 <= len(endpoint) <= 2048:
        raise ValueError("Invalid public MCP endpoint")
    if any(ord(char) < 33 or char == "\\" for char in endpoint):
        raise ValueError("Invalid public MCP endpoint")
    url = httpx.URL(endpoint)
    host = url.raw_host.decode("ascii")
    if (url.scheme != "https" or url.port not in (None, 443) or url.userinfo
            or url.fragment or url.query or not _DNS_NAME.fullmatch(host)
            or host.endswith((".local", ".localhost", ".internal"))):
        raise ValueError("An agent-installed MCP requires public HTTPS DNS without credentials")
    return url


async def _ipv4_addresses(host):
    records = await asyncio.wait_for(asyncio.get_running_loop().getaddrinfo(
        host, 443, family=socket.AF_INET, type=socket.SOCK_STREAM), timeout=5)
    return tuple(dict.fromkeys(record[4][0] for record in records))


class _PublicHTTPSTransport(httpx.AsyncBaseTransport):
    def __init__(self, endpoint, transport, resolver=_ipv4_addresses):
        self._endpoint = validate_public_https(endpoint)
        self._transport = transport
        self._resolver = resolver

    async def handle_async_request(self, request):
        if request.url != self._endpoint:
            raise httpx.ConnectError("MCP request changed its reviewed HTTPS target", request=request)
        try:
            addresses = await self._resolver(request.url.host)
            public = tuple(ipaddress.IPv4Address(value) for value in addresses)
            if not public or any(not ip.is_global or ip.is_multicast or ip.is_reserved for ip in public):
                raise ValueError("The MCP hostname did not resolve exclusively to public IPv4")
        except (OSError, ValueError, TimeoutError) as exc:
            raise httpx.ConnectError("MCP public DNS admission failed", request=request) from exc
        # The connection pool dials only this validated numeric IP. Host and
        # TLS verification still use the original DNS name.
        headers = request.headers.copy()
        headers["Host"] = request.url.host
        extensions = {key: value for key, value in request.extensions.items() if key == "timeout"}
        extensions["sni_hostname"] = request.url.host
        pinned = httpx.Request(request.method, request.url.copy_with(host=addresses[0]),
                               headers=headers, stream=request.stream, extensions=extensions)
        response = await self._transport.handle_async_request(pinned)
        if 300 <= response.status_code < 400:
            await response.aclose()
            raise httpx.ConnectError("MCP endpoint redirects are unavailable", request=request)
        return response

    async def aclose(self):
        await self._transport.aclose()


def public_https_client(endpoint, headers=None, timeout=None, auth=None):
    return httpx.AsyncClient(
        headers=headers, auth=auth, trust_env=False, follow_redirects=False,
        timeout=timeout if timeout is not None else httpx.Timeout(30, read=300),
        transport=_PublicHTTPSTransport(endpoint, httpx.AsyncHTTPTransport(trust_env=False)),
    )
