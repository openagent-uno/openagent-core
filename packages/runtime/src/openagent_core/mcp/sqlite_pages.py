"""Shared keyset paging for tool queries with a unique text ID."""

from __future__ import annotations

import base64
import hashlib
import json
import math
from typing import Any

from openagent_core.mcp.pagination import collection_page, page_character_budget


async def select_page(
    connection, table: str, columns: str, *, order_by: str, limit: int,
    cursor: str | None, conditions: list[str], parameters: list[Any],
    newest_first: bool = True, max_limit: int = 50,
) -> tuple[list[dict[str, Any]], str | None]:
    """Apply an opaque continuation after all filters, with an ID tiebreaker.

    SQL fragments and the sort expression come from the tool implementation.
    Caller values are bound separately. A cursor conveys position only.
    """
    if type(limit) is not int or not 1 <= limit <= max_limit:
        raise ValueError(f"Page limit must be between 1 and {max_limit}")
    scope = hashlib.sha256(
        json.dumps([table, columns, order_by, newest_first, conditions, parameters]).encode()
    ).hexdigest()
    direction, comparison = ("DESC", "<") if newest_first else ("ASC", ">")
    if cursor is not None:
        try:
            if not isinstance(cursor, str) or not 1 <= len(cursor) <= 4096:
                raise ValueError()
            token = base64.b64decode(cursor, altchars=b"-_", validate=True)
            scope_id, value, identifier = json.loads(token)
            valid_value = isinstance(value, str) or (
                type(value) in (int, float) and math.isfinite(value)
            )
            if scope_id != scope or not valid_value or not isinstance(identifier, str):
                raise ValueError()
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Invalid page cursor. Use the same filters or start again.") from exc
        conditions = [*conditions, f"({order_by} {comparison} ? OR ({order_by} = ? AND id > ?))"]
        parameters = [*parameters, value, value, identifier]

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    async with connection.execute(
        f"SELECT {columns}, {order_by} AS _page_sort FROM {table} {where} "
        f"ORDER BY _page_sort {direction}, id ASC LIMIT ?",
        (*parameters, limit + 1),
    ) as result:
        rows = await result.fetchall()
    # Keep a small budget before decoration. An individual oversized record
    # still advances the cursor and remains subject to the provider ceiling.
    page = collection_page([dict(row) for row in rows], 0, limit, page_character_budget())
    next_cursor = None
    if len(rows) > len(page):
        last = page[-1]
        token = json.dumps([scope, last["_page_sort"], last["id"]], separators=(",", ":")).encode()
        next_cursor = base64.urlsafe_b64encode(token).decode()
    for row in page:
        del row["_page_sort"]
    return page, next_cursor
