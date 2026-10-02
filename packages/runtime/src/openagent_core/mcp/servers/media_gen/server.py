"""Media generation MCP server.

Exposes ``generate_image`` and ``generate_video`` to the agent so it can
render visuals inline (Telegram replies, vault notes, workflow outputs).
Subprocess MCP (stdio transport) — same pattern as the scheduler.

Providers:
  • image:  any OpenAI-compatible ``/v1/images/generations`` endpoint
  • video:  ``fal``     (pixverse / runway etc.)  — uses ``FAL_KEY``

The image backend is resolved from an enabled image model in the catalog.
For older catalogs, an LLM model declaring ``image_generation`` in metadata
is also accepted. The host configures provider credentials and decides which
models are enabled.

Resolution order:
  1. ``OPENAGENT_IMAGE_BASE_URL`` (+ ``_API_KEY`` / ``_MODEL``) — explicit override.
  2. an enabled image model (or legacy capable model) → its provider's base_url.
  3. ``OPENAI_API_KEY`` → api.openai.com, the metered fallback.

Generation stays a TOOL rather than a routing decision on purpose. Making
it a model choice would mean a turn that needs reasoning AND a picture
could not exist — the agent would have to abandon the model mid-thought to
draw. As a tool, every model in the roster can produce images.

When nothing is configured, the tool surfaces a clear "not configured"
error instead of silently failing — the model can fall back or tell the user.

Files are written under ``~/.cache/openagent/media/<timestamp>-<hash>.{ext}``.
The tool returns the local path and the outbound image marker; hosts decide
whether to deliver that marker to a channel or retain the file for later use.
"""

from __future__ import annotations

from openagent_core.configuration import runtime_environment
import asyncio
import hashlib
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP

from openagent_core.mcp.servers._common import SharedConnection

logger = logging.getLogger(__name__)

mcp = FastMCP("media-gen")


_CACHE_DIR = Path(os.path.expanduser("~/.cache/openagent/media"))

_IMAGE_CAPABILITY = "image_generation"
_conn = SharedConnection("media-gen")


def _images_url(base_url: str) -> str:
    """Build the images endpoint from a provider base_url.

    Providers are stored either as ``https://host/v1`` or ``https://host``;
    both must land on the same place, and a double ``/v1/v1`` 404s in a way
    that reads like the endpoint does not exist.
    """
    base = (base_url or "").rstrip("/")
    if not base:
        return ""
    if base.endswith("/v1"):
        return f"{base}/images/generations"
    return f"{base}/v1/images/generations"


async def _capability_backend(requested_model: str = "") -> Optional[tuple[str, str, str]]:
    """Find an enabled model that declares image generation.

    Returns ``(images_url, api_key, model_id)`` or None. The capability lives
    in the model's metadata, which is the same field the runtime already
    hydrates onto the catalog — so declaring it once makes it visible both
    here and to anything that later routes on it.
    """
    try:
        conn = await _conn.get()
        rows = await (await conn.execute(
            """
            SELECT p.base_url AS base_url, p.api_key AS api_key, p.name AS provider,
                   m.model AS model, m.kind AS kind,
                   m.metadata_json AS metadata_json
            FROM models m JOIN providers p ON p.id = m.provider_id
            WHERE m.enabled = 1 AND p.enabled = 1
              AND (m.kind = 'image' OR m.kind = 'llm')
            ORDER BY CASE WHEN m.kind = 'image' THEN 0 ELSE 1 END, m.id
            """
        )).fetchall()
    except Exception as e:  # noqa: BLE001 - never fail generation over lookup
        logger.debug("media-gen capability lookup failed: %s", e)
        return None

    import json as _json

    for row in rows or []:
        if requested_model and requested_model not in {
            str(row["model"] or ""), f'{row["provider"]}:{row["model"]}',
        }:
            continue
        try:
            meta = _json.loads(row["metadata_json"] or "{}")
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(meta, dict):
            continue
        from openagent_core.image_generation import supports_image_generation, provider_image_model_id
        if not supports_image_generation(str(row["kind"] or "llm"), meta):
            continue
        from openagent_core.image_generation import images_endpoint
        try:
            url = images_endpoint(str(row["base_url"] or ""), str(row["provider"] or ""))
        except ValueError:
            continue
        if url:
            # A host may register a qualified catalog id to disambiguate
            # variants while the provider's images API requires its bare id.
            image_model_id = provider_image_model_id(str(row["model"] or ""), meta)
            return url, str(row["api_key"] or ""), image_model_id
    return None


async def _image_backend(model: str) -> tuple[Optional[tuple[str, str, str]], Optional[str]]:
    """Resolve where to send an image request. Returns ``(backend, reason)``."""
    base = (runtime_environment().get("OPENAGENT_IMAGE_BASE_URL") or "").strip()
    if base:
        return (
            _images_url(base),
            (runtime_environment().get("OPENAGENT_IMAGE_API_KEY") or "").strip(),
            (runtime_environment().get("OPENAGENT_IMAGE_MODEL") or model or "").strip(),
        ), None

    backend = await _capability_backend(model)
    if backend:
        return backend, None

    api_key = (runtime_environment().get("OPENAI_API_KEY") or "").strip()
    if api_key:
        return ("https://api.openai.com/v1/images/generations", api_key, model), None

    return None, (
        "no image backend: no enabled model declares the "
        f"'{_IMAGE_CAPABILITY}' capability, OPENAGENT_IMAGE_BASE_URL is unset, "
        "and OPENAI_API_KEY is not configured."
    )


def _cache_path(prefix: str, prompt: str, ext: str) -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256(prompt.encode("utf-8", errors="ignore")).hexdigest()[:8]
    ts = int(time.time())
    return _CACHE_DIR / f"{prefix}-{ts}-{h}.{ext}"


async def _save_url_to_file(url: str, dest: Path, timeout_s: float = 60.0) -> bool:
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            r = await client.get(url)
            r.raise_for_status()
            dest.write_bytes(r.content)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("media-gen download failed: %s", e)
        return False


# ── Image generation ─────────────────────────────────────────────────


@mcp.tool()
async def generate_image(
    prompt: str,
    model: str = "",
    size: str = "1024x1024",
    quality: str = "auto",
) -> dict[str, Any]:
    """Generate an image from a text prompt.

    Args:
        prompt: What to draw — natural-language description.
        model: Optional model id. The configured image model is used by default.
        size: ``1024x1024`` / ``1536x1024`` / ``1024x1536``. Honoured on a
            best-effort basis — the returned ``size`` is what was actually
            produced, which is not always what was asked for.
        quality: ``auto``, ``low``, ``medium`` or ``high``; the newest GPT image
            variants additionally accept ``xhigh`` and ``max``.

    Returns ``{ok, local_path, remote_url, model, size}``. ``ok`` is False
    with a ``reason`` field when no backend is configured or the request
    failed."""
    backend, reason = await _image_backend(model)
    if backend is None:
        return {"ok": False, "reason": reason}
    url, api_key, backend_model = backend
    from openagent_core.image_generation import generate_image_bytes
    try:
        image = await generate_image_bytes(
            endpoint=url, api_key=api_key, model=backend_model or model or "gpt-image-1",
            prompt=prompt, size=size, quality=quality,
        )
    except Exception as exc:  # noqa: BLE001 - an MCP failure is a tool result
        return {"ok": False, "reason": f"image generation failed: {exc}"}
    extension = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp"}[image.mime_type]
    local = _cache_path("img", prompt, extension)
    local.write_bytes(image.content)

    return {
        "ok": True,
        "local_path": str(local),
        "remote_url": image.remote_url,
        "model": image.model,
        "send_marker": f"[IMAGE:{local}]",
        "send_instruction": "Include send_marker verbatim in the final reply to deliver the image in the current channel. Omit it to keep the image for later use.",
        # What came back, not what was asked for.
        "size": image.size,
        "bytes": local.stat().st_size,
    }


# ── Video generation ─────────────────────────────────────────────────


@mcp.tool()
async def generate_video(
    prompt: str,
    model: str = "fal-ai/pixverse/v3.5/text-to-video",
    duration_s: int = 5,
) -> dict[str, Any]:
    """Generate a short video from a text prompt via Fal.

    Args:
        prompt: What to render.
        model: A Fal model id (default Pixverse v3.5 text-to-video).
        duration_s: Length in seconds (most models cap at 5-8).

    Returns ``{ok, local_path, remote_url}`` with ``ok=False`` and a
    ``reason`` when the Fal key is missing or generation fails."""
    api_key = (runtime_environment().get("FAL_KEY") or runtime_environment().get("FAL_API_KEY") or "").strip()
    if not api_key:
        return {
            "ok": False,
            "reason": "FAL_KEY not set — configure under "
                      "media_gen.fal.api_key or as env var.",
        }
    # Minimal direct HTTP submit — keeps the dependency surface small
    # (no fal-client SDK requirement). For long-running jobs we poll
    # the queue endpoint, capped at ``_QUEUE_TIMEOUT_S``.
    _QUEUE_TIMEOUT_S = 180
    headers = {"Authorization": f"Key {api_key}", "Content-Type": "application/json"}
    payload = {"prompt": prompt[:2000], "duration": str(duration_s)}
    submit_url = f"https://queue.fal.run/{model}"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.post(submit_url, headers=headers, json=payload)
            r.raise_for_status()
            queue = r.json()
            status_url = queue.get("status_url")
            response_url = queue.get("response_url")
            if not status_url or not response_url:
                return {"ok": False, "reason": f"Fal returned no status_url: {queue}"}
            # Poll until completion.
            deadline = time.monotonic() + _QUEUE_TIMEOUT_S
            while time.monotonic() < deadline:
                sr = await client.get(status_url, headers=headers)
                sr.raise_for_status()
                status_payload = sr.json()
                if status_payload.get("status") == "COMPLETED":
                    final = await client.get(response_url, headers=headers)
                    final.raise_for_status()
                    data = final.json()
                    video = (data.get("video") or {})
                    url = video.get("url") or data.get("url")
                    if not url:
                        return {"ok": False, "reason": f"Fal completed without video url: {data}"}
                    local = _cache_path("vid", prompt, "mp4")
                    if not await _save_url_to_file(url, local, timeout_s=120):
                        return {"ok": False, "reason": "failed to download generated video"}
                    return {"ok": True, "local_path": str(local), "remote_url": url}
                if status_payload.get("status") == "FAILED":
                    return {"ok": False, "reason": f"Fal job failed: {status_payload}"}
                await asyncio.sleep(2.0)
        return {"ok": False, "reason": "Fal job timed out"}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reason": f"Fal request failed: {e}"}


def main() -> None:
    """Entry point matched by ``builtins.py`` python_module pattern."""
    mcp.run()


if __name__ == "__main__":
    main()
