"""Keep MCP pixels once in the durable envelope, without changing the wire data.

The runtime ToolResult has both the original MCP content and SDK Image aliases.
Core's envelope codec enriched the former with the latter, doubling every image
before storing the result four times. Drop only byte-identical SDK aliases;
native JSON, canonical MCP blocks and non-duplicate media remain untouched.
"""
from __future__ import annotations

from typing import Any


def deduplicate_image_aliases(envelope: Any) -> Any:
    if not isinstance(envelope, dict) or not isinstance(envelope.get("images"), list):
        return envelope
    blocks = envelope.get("content")
    if not isinstance(blocks, list):
        return envelope
    canonical = {
        (block.get("mimeType"), block.get("data"))
        for block in blocks
        if isinstance(block, dict) and block.get("type") == "image"
        and isinstance(block.get("data"), str) and block.get("data")
        and isinstance(block.get("mimeType"), str)
    }
    if not canonical:
        return envelope
    def redundant(image):
        if not isinstance(image, dict):
            return False
        # An SDK-generated id is just a rendering handle. Preserve meaningful
        # extra metadata, remote/file media and anything we cannot prove equal.
        permitted = {"id", "content", "mime_type", "format", "alt_text", "detail",
                     "filepath", "url", "original_prompt", "revised_prompt"}
        if set(image) - permitted:
            return False
        if any(image.get(k) for k in permitted - {"id", "content", "mime_type"}):
            return False
        data, mime = image.get("content"), image.get("mime_type")
        return isinstance(data, str) and isinstance(mime, str) and (mime, data) in canonical
    kept = [image for image in envelope["images"] if not redundant(image)]
    if len(kept) == len(envelope["images"]):
        return envelope
    compact = dict(envelope)
    if kept:
        compact["images"] = kept
    else:
        compact.pop("images")
    return compact
