"""Provider-neutral image generation transport shared by OpenAgent hosts."""
from __future__ import annotations

import base64
from dataclasses import dataclass, replace
from urllib.parse import urlparse

import httpx


MAX_IMAGE_BYTES = 20 << 20


def supports_image_generation(kind: str, metadata: object) -> bool:
    """Whether a catalog row may serve the shared image generation tool."""
    if kind == "image":
        return True
    capabilities = metadata.get("capabilities") if isinstance(metadata, dict) else None
    return (kind == "llm" and isinstance(capabilities, (list, tuple))
            and "image_generation" in capabilities)


def provider_image_model_id(model_id: str, metadata: object) -> str:
    """Resolve a provider's bare image ID from a qualified catalog identity."""
    alias = metadata.get("image_model_id") if isinstance(metadata, dict) else None
    return alias.strip() if isinstance(alias, str) and alias.strip() else model_id


class ImageGenerationError(Exception):
    """The image provider refused a request; carries what it said and why.

    ``kind`` is the provider's own error type or code (for example an expired
    sign-in or an exhausted quota), so a caller never has to guess the cause
    from the HTTP status alone.
    """

    def __init__(self, status: int, message: str = "", kind: str = "") -> None:
        self.status, self.kind = status, kind
        text = f"the image provider answered HTTP {status}"
        if kind:
            text += f" ({kind})"
        super().__init__(f"{text}: {message}" if message else text)


def _provider_error(response: httpx.Response) -> ImageGenerationError:
    """What the provider said, bounded: its message and its error type."""
    message = kind = ""
    try:
        body = response.json()
    except ValueError:
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        message = error.get("message") if isinstance(error.get("message"), str) else ""
        named = [error.get(key) for key in ("type", "code")]
        kind = next((value for value in named if isinstance(value, str) and value.strip()), "")
    elif isinstance(error, str):
        message = error
    elif isinstance(body, dict):
        detail = body.get("detail") or body.get("message")
        message = detail if isinstance(detail, str) else ""
    message = " ".join(message.split())
    if len(message) > 600:
        message = message[:599].rstrip() + "…"
    return ImageGenerationError(response.status_code, message, " ".join(kind.split())[:80])


@dataclass(frozen=True)
class GeneratedImage:
    content: bytes
    mime_type: str
    model: str
    size: str
    remote_url: str | None = None
    connection: str | None = None
    connection_label: str | None = None
    account_id: str | None = None


def images_endpoint(base_url: str, provider: str = "") -> str:
    base = base_url.rstrip("/")
    if not base and provider.lower() == "openai":
        base = "https://api.openai.com/v1"
    if not base:
        raise ValueError("The image provider needs an images API base URL")
    if urlparse(base).scheme not in {"https", "http"}:
        raise ValueError("The image provider URL must use HTTP or HTTPS")
    return f"{base}/images/generations" if base.endswith("/v1") else f"{base}/v1/images/generations"


async def generate_image_bytes(
    *, endpoint: str, api_key: str, model: str, prompt: str,
    size: str = "1024x1024", quality: str = "auto", client: httpx.AsyncClient | None = None,
) -> GeneratedImage:
    if not prompt.strip() or len(prompt) > 4000:
        raise ValueError("An image prompt of at most 4000 characters is required")
    if size not in {"1024x1024", "1536x1024", "1024x1536"}:
        raise ValueError("Unsupported image size")
    if quality not in {"auto", "low", "medium", "high", "xhigh", "max"}:
        raise ValueError("Unsupported image quality")
    if urlparse(endpoint).scheme not in {"https", "http"}:
        raise ValueError("The image endpoint must use HTTP or HTTPS")
    payload = {"model": model, "prompt": prompt, "n": 1, "size": size}
    if endpoint.startswith("https://api.openai.com/"):
        payload["quality"] = quality
    headers = {"content-type": "application/json"}
    if api_key:
        headers["authorization"] = f"Bearer {api_key}"
    from contextlib import asynccontextmanager
    @asynccontextmanager
    async def route():
        if client is not None:
            yield client
        else:
            async with httpx.AsyncClient(timeout=600.0, follow_redirects=False) as owned:
                yield owned
    async with route() as routed_client:
        try:
            response = await routed_client.post(endpoint, headers=headers, json=payload)
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            # Only a failed generation connection is safe to move elsewhere.
            # An accepted generation whose result URL fails to download must
            # retain that job rather than silently purchasing another image.
            error.generation_not_accepted = True
            raise
        if response.status_code >= 400:
            raise _provider_error(response)
        body = response.json()
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            raise ValueError("The image backend returned no image")
        item = data[0]
        remote_url = item.get("url")
        encoded = item.get("b64_json")
        if isinstance(encoded, str) and encoded:
            if len(encoded) > (MAX_IMAGE_BYTES * 4 // 3) + 8:
                raise ValueError("The generated image is too large")
            content = base64.b64decode(encoded, validate=True)
        elif isinstance(remote_url, str) and remote_url:
            # Provider-supplied download URLs are accepted only over HTTPS.
            # Limit bytes while streaming to avoid unbounded disk/memory use.
            if urlparse(remote_url).scheme != "https":
                raise ValueError("The image backend returned an insecure download URL")
            async with httpx.AsyncClient(timeout=600.0, follow_redirects=False) as downloader, downloader.stream("GET", remote_url) as download:
                download.raise_for_status()
                chunks = bytearray()
                async for chunk in download.aiter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > MAX_IMAGE_BYTES:
                        raise ValueError("The generated image is too large")
                content = bytes(chunks)
        else:
            raise ValueError("The image backend returned neither bytes nor a URL")
    if len(content) > MAX_IMAGE_BYTES or not content:
        raise ValueError("The generated image is empty or too large")
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        mime = "image/png"
    elif content.startswith(b"\xff\xd8\xff"):
        mime = "image/jpeg"
    elif content.startswith(b"RIFF") and content[8:12] == b"WEBP":
        mime = "image/webp"
    else:
        raise ValueError("The image backend returned an unsupported image format")
    return GeneratedImage(content, mime, model, str(item.get("size") or size),
                          remote_url if isinstance(remote_url, str) else None)


async def generate_catalog_image(db, *, prompt, size="1024x1024", quality="auto",
                                 model_id=None, exact=False, budget_guard=None):
    """Recover an automatic image request through authorized typed catalog rows."""
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 4000:
        raise ValueError("An image prompt of at most 4000 characters is required")
    if size not in {"1024x1024", "1536x1024", "1024x1536"}:
        raise ValueError("Unsupported image size")
    if quality not in {"auto", "low", "medium", "high", "xhigh", "max"}:
        raise ValueError("Unsupported image quality")
    from .models.catalog_routes import catalog_routes, route_failed, route_succeeded
    rows = await catalog_routes(db, "image", preferred_id=model_id, guard=budget_guard)
    if exact:
        rows = [row for row in rows if row["id"] == model_id]
    last = None
    for row in rows:
        provider = await db.get_provider(row["provider_id"])
        metadata = provider.get("metadata") or {}
        wire_model = provider_image_model_id(row["model"], row.get("metadata"))
        if metadata.get("access") != "subscription":
            try:
                endpoint = images_endpoint(provider.get("base_url") or "", metadata.get("vendor") or provider["name"])
            except ValueError as error:
                if exact:
                    raise
                last = error
                continue
        try:
            if metadata.get("access") == "subscription":
                from .subscriptions.images import generate_subscription_image
                image = await generate_subscription_image(db, provider["id"], model_id=row["id"],
                    model=wire_model, prompt=prompt, size=size, quality=quality, budget_guard=budget_guard)
            else:
                image = await generate_image_bytes(endpoint=endpoint, api_key=provider.get("api_key") or "",
                    model=wire_model, prompt=prompt, size=size, quality=quality)
                image = replace(image, connection=provider["name"], connection_label=metadata.get("label") or provider["name"])
        except ImageGenerationError as error:
            from types import SimpleNamespace
            from .models.automatic_fallback import unavailable_model_error
            recoverable = (error.status in {401,429,529} or
                unavailable_model_error(SimpleNamespace(status_code=error.status, message=str(error))))
            if exact or not recoverable:
                raise
            last = error
            route_failed(db, "image", row)
            continue
        except (httpx.ConnectError, httpx.ConnectTimeout) as error:
            if exact or not getattr(error, 'generation_not_accepted', False):
                raise
            last = error
            route_failed(db, "image", row)
            continue
        route_succeeded(db, "image", row)
        return image
    if last is not None:
        raise last
    raise ImageGenerationError(429, "No enabled image model is eligible for this request", "image_model_unavailable")
