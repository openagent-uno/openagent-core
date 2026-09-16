from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Dict, Optional


def isolated_http_client(headers=None, timeout=None, auth=None):
    """MCP destinations and credentials come from the host's explicit config."""
    import httpx
    return httpx.AsyncClient(headers=headers, auth=auth, trust_env=False,
        timeout=timeout if timeout is not None else httpx.Timeout(30, read=300))


@dataclass
class SSEClientParams:
    """Parameters for SSE client connection."""

    url: str
    headers: Optional[Dict[str, Any]] = None
    timeout: Optional[float] = 5
    sse_read_timeout: Optional[float] = 60 * 5
    httpx_client_factory: Callable[..., Any] = isolated_http_client


@dataclass
class StreamableHTTPClientParams:
    """Parameters for Streamable HTTP client connection."""

    url: str
    headers: Optional[Dict[str, Any]] = None
    timeout: Optional[timedelta] = timedelta(seconds=30)
    sse_read_timeout: Optional[timedelta] = timedelta(seconds=60 * 5)
    terminate_on_close: Optional[bool] = None
    httpx_client_factory: Callable[..., Any] = isolated_http_client
