"""Shared size budget for tool collection pages."""

from __future__ import annotations

import json

from openagent_core.core.tool_output import max_tool_result_chars


def page_character_budget() -> int:
    limit = max_tool_result_chars()
    return min(8_000, max(200, limit - 600)) if limit > 0 else 8_000


def collection_page(items: list, start: int, limit: int, characters: int) -> list:
    """Take complete items. Allow one oversized item so the cursor advances."""
    size = 2
    end = start
    for item in items[start:start + limit]:
        cost = len(json.dumps(item, ensure_ascii=True, separators=(",", ":"))) + 1
        if end > start and size + cost > characters:
            break
        end += 1
        size += cost
    return items[start:end]
