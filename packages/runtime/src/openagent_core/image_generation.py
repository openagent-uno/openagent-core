"""Provider-neutral image generation transport shared by OpenAgent hosts."""
from __future__ import annotations

import base64
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx


MAX_IMAGE_BYTES = 20 << 20


@dataclass(frozen=True)
class GeneratedImage:
    content: bytes
    mime_type: str
    model: str
    size: str
    remote_url: str | None = None


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
    size: str = "1024x1024", quality: str = "auto",
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
    async with httpx.AsyncClient(timeout=600.0, follow_redirects=False) as client:
        response = await client.post(endpoint, headers=headers, json=payload)
        response.raise_for_status()
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
            async with client.stream("GET", remote_url) as download:
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
