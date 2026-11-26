"""Attested presentation of text around canonical transcript tool rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from openagent_core.memory.message_parts import (
    MAX_MESSAGE_PARTS,
    canonical_parts_for_messages_on_connection,
)
from openagent_core.memory.operational.access import AccessContext, resource_is_visible
from openagent_core.stream.content_parts import parse_response_content


_MAX_PAGE_MESSAGES = 256
_MAX_TURN_MESSAGES = 512
_MAX_PAGE_TURN_ROWS = 4096
_MAX_PAGE_PARTS = 2048
_MAX_TURN_EVENTS = 4096
_MAX_PAGE_EVENTS = 12288
_MAX_TURN_TEXT_BYTES = 512 * 1024
_MAX_PAGE_TEXT_BYTES = 2 * 1024 * 1024


class TranscriptSegment(TypedDict):
    text: str
    parts: list[dict[str, Any]]


async def transcript_segments(
    conn: Any,
    *,
    access: AccessContext,
    session_id: str,
    messages: Sequence[Mapping[str, Any]],
) -> dict[str, TranscriptSegment]:
    """Return optional display segments for authorized rows of one page.

    The terminal assistant row and all tool anchors of a candidate run are
    checked even if they lie outside the page. Keep the canonical messages
    intact. A missing key means to render that message as usual.
    """

    if not messages or len(messages) > _MAX_PAGE_MESSAGES:
        return {}
    page: dict[str, dict[str, str]] = {}
    for message in messages:
        run_id, message_id, role = (message.get(key) for key in ("run_id", "id", "role"))
        if isinstance(run_id, str) and isinstance(message_id, str) and role in ("tool", "assistant"):
            page.setdefault(run_id, {})[message_id] = role
    if not page:
        return {}
    session = await (
        await conn.execute(
            "SELECT id AS resource_id, 'session' AS resource_type, tenant_id, "
            "owner_principal_id, visibility, acl_version FROM sessions_v2 "
            "WHERE id=? AND deleted_at_ms IS NULL",
            (session_id,),
        )
    ).fetchone()
    if session is None or not await resource_is_visible(conn, session, access):
        return {}

    placeholders = ",".join("?" for _ in page)
    runs = await (
        await conn.execute(
            "SELECT r.id FROM session_runs r "
            f"WHERE r.id IN ({placeholders}) AND r.tenant_id=? AND r.session_id=? "
            "AND r.status='success' AND r.finished_at_ms IS NOT NULL "
            "AND json_extract(r.metadata_json,'$.runtime_contract')=1 "
            "AND EXISTS (SELECT 1 FROM session_messages m WHERE m.run_id=r.id AND m.role='tool')",
            (*page, access.tenant_id, session_id),
        )
    ).fetchall()
    if not runs:
        return {}
    tables = {
        row["name"]
        for row in await (
            await conn.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' "
                "AND name IN ('session_message_parts', 'ui_views', 'ui_message_links')"
            )
        ).fetchall()
    }
    turns: dict[str, Sequence[Any]] = {}
    remaining_text = _MAX_PAGE_TEXT_BYTES
    remaining_rows = _MAX_PAGE_TURN_ROWS
    remaining_parts = _MAX_PAGE_PARTS
    for run in runs:
        if remaining_rows < 2:
            break
        limit = min(remaining_rows, _MAX_TURN_MESSAGES)
        rows = await _turn_messages(
            conn, access.tenant_id, session_id, run["id"], tables, limit,
        )
        remaining_rows -= len(rows)
        if len(rows) > limit or not _can_layout(rows):
            continue
        answer = rows[-1]
        size = max(answer["text_bytes"], answer["part_bytes"])
        if size > remaining_text or answer["part_count"] > remaining_parts:
            continue
        remaining_text -= size
        remaining_parts -= answer["part_count"]
        turns[run["id"]] = rows

    parts_by_id = await canonical_parts_for_messages_on_connection(
        conn,
        [rows[-1]["id"] for rows in turns.values() if rows[-1]["part_count"]],
        access=access if "ui_views" in tables else None,
    )
    result: dict[str, TranscriptSegment] = {}
    remaining_events = _MAX_PAGE_EVENTS
    for run_id, rows in turns.items():
        if remaining_events <= 0:
            break
        answer = rows[-1]
        parts = parts_by_id.get(answer["id"], [])
        canonical_text = _attestable_text(answer, parts)
        if canonical_text is None:
            continue
        if not answer["part_count"] and canonical_text:
            parts = [{"kind": "text", "text": canonical_text}]
        anchors = {row["id"]: row["role"] for row in rows}
        if not any(anchors.get(key) == role for key, role in page[run_id].items()):
            continue
        cuts, spent = await _attest_journal(
            conn, access.tenant_id, session_id, run_id, rows, canonical_text,
            min(remaining_events, _MAX_TURN_EVENTS),
        )
        remaining_events -= spent
        if cuts is None:
            continue
        segments = _split_parts(parts, [*cuts, len(canonical_text)])
        for row, segment in zip(rows, segments, strict=True):
            if page[run_id].get(row["id"]) == row["role"]:
                result[row["id"]] = segment
    return result


async def _turn_messages(
    conn: Any, tenant_id: str, session_id: str, run_id: str, tables: set[str], limit: int
) -> Sequence[Any]:
    # The tool result lives in m.text too. Select text only from the assistant
    # row and do not load invocation arguments, results or provider envelopes.
    part_count = part_bytes = "0"
    if "session_message_parts" in tables:
        part_count = "(SELECT count(*) FROM session_message_parts p WHERE p.message_id=m.id)"
        part_bytes = (
            "(SELECT coalesce(sum(length(CAST(p.text_content AS BLOB))),0) "
            "FROM session_message_parts p WHERE p.message_id=m.id)"
        )
    rich_views = ""
    if "ui_message_links" in tables:
        rich_views = (
            "OR EXISTS(SELECT 1 FROM ui_message_links l WHERE l.tenant_id=m.tenant_id "
            "AND l.session_id=m.session_id AND l.message_id=m.id)"
        )
    return await (
        await conn.execute(
            "SELECT m.id, m.role, m.sequence, m.status, m.visibility, m.tool_call_id, "
            "m.legacy_inferred, m.source_version, "
            "CASE WHEN m.role='assistant' AND length(CAST(m.text AS BLOB))<=? THEN m.text END AS text, "
            "CASE WHEN m.role='assistant' THEN length(CAST(m.text AS BLOB)) END AS text_bytes, "
            "CASE WHEN m.role='assistant' THEN json_extract(m.raw_envelope_json,'$.privacy_redacted') END AS redacted, "
            f"CASE WHEN m.role='assistant' THEN {part_count} END AS part_count, "
            f"CASE WHEN m.role='assistant' THEN {part_bytes} END AS part_bytes, "
            "CASE WHEN m.role='assistant' THEN (EXISTS(SELECT 1 FROM artifact_links l "
            "WHERE l.tenant_id=m.tenant_id AND l.resource_type='message' AND l.resource_id=m.id)"
            f" {rich_views}) END AS has_rich_links "
            "FROM session_messages m "
            "WHERE m.run_id=? AND m.tenant_id=? AND m.session_id=? AND m.role IN ('assistant','tool') "
            "ORDER BY m.ordinal LIMIT ?",
            (_MAX_TURN_TEXT_BYTES, run_id, tenant_id, session_id, limit + 1),
        )
    ).fetchall()


def _can_layout(rows: Sequence[Any]) -> bool:
    if not 2 <= len(rows) <= _MAX_TURN_MESSAGES or rows[-1]["role"] != "assistant":
        return False
    answer = rows[-1]
    if (
        answer["text"] is None or answer["redacted"] or answer["source_version"] != 1
        or answer["part_count"] > MAX_MESSAGE_PARTS or answer["part_bytes"] > _MAX_TURN_TEXT_BYTES
    ):
        return False
    if any(
        row["status"] != "complete" or row["visibility"] != "user_visible" or row["legacy_inferred"]
        for row in rows
    ):
        return False
    calls = [row["tool_call_id"] for row in rows[:-1]]
    return (
        all(row["role"] == "tool" for row in rows[:-1])
        and all(calls)
        and len(set(calls)) == len(calls)
        and all(left["sequence"] < right["sequence"] for left, right in zip(rows, rows[1:]))
    )


def _attestable_text(answer: Any, parts: Sequence[Mapping[str, Any]]) -> str | None:
    raw = answer["text"]
    public_text = parse_response_content(raw, allow_inline_ui=True).text
    if answer["part_count"]:
        # Missing rich parts were filtered by their current ACL. Do not move
        # any surviving parts from this assistant row in that case.
        if len(parts) != answer["part_count"]:
            return None
        text = "".join(part["text"] for part in parts if part["kind"] == "text")
        return text if text == public_text else None
    return public_text if raw == public_text and not answer["has_rich_links"] else None


async def _attest_journal(
    conn: Any,
    tenant_id: str,
    session_id: str,
    run_id: str,
    rows: Sequence[Any],
    text: str,
    limit: int,
) -> tuple[list[int] | None, int]:
    # The ordered, bounded run index supplies all events, including unknown
    # markers. A SQL projection keeps unrelated journal data out of Python.
    query = """
        SELECT event_type, sensitivity, json_extract(metadata_json,'$.privacy_redacted') AS redacted,
               CASE WHEN event_type='run.stream' THEN json_extract(metadata_json,'$.kind') END AS stream_kind,
               CASE WHEN event_type='run.stream' AND sensitivity='normal'
                    AND json_type(metadata_json,'$.text')='text'
                    AND length(CAST(json_extract(metadata_json,'$.text') AS BLOB))<=?
                    THEN json_extract(metadata_json,'$.text') END AS text,
               CASE WHEN event_type IN ('tool.invoking','tool.completed') AND sensitivity='normal'
                    THEN json_extract(metadata_json,'$.call_id') END AS call_id
        FROM domain_events
        WHERE run_id=? AND tenant_id=? AND session_id=? AND resource_type='run' AND resource_id=?
        ORDER BY sequence LIMIT ?
    """
    tool_ids = [row["tool_call_id"] for row in rows[:-1]]
    cuts: list[int] = []
    outstanding: set[str] = set()
    offset = spent = 0
    state = "new"
    parameters = (_MAX_TURN_TEXT_BYTES, run_id, tenant_id, session_id, run_id, limit + 1)
    async with conn.execute(query, parameters) as cursor:
        async for event in cursor:
            spent += 1
            if spent > limit or event["redacted"] or event["sensitivity"] != "normal":
                return None, spent
            kind = event["event_type"]
            if kind == "run.accepted" and state == "new":
                state = "accepted"
            elif kind == "run.running" and state == "accepted":
                state = "running"
            elif kind == "run.success" and state == "done":
                state = "success"
            elif kind == "run.stream" and event["stream_kind"] == "done":
                if (
                    state != "running" or event["text"] != text or offset != len(text)
                    or len(cuts) != len(tool_ids) or outstanding
                ):
                    return None, spent
                state = "done"
            elif kind == "run.stream" and event["stream_kind"] == "delta":
                chunk = event["text"]
                if state != "running" or chunk is None or not text.startswith(chunk, offset):
                    return None, spent
                offset += len(chunk)
            elif kind == "run.stream" and event["stream_kind"] in (None, "error"):
                return None, spent
            elif kind == "tool.invoking":
                call = event["call_id"]
                if state != "running" or len(cuts) >= len(tool_ids) or call != tool_ids[len(cuts)]:
                    return None, spent
                cuts.append(offset)
                outstanding.add(call)
            elif kind == "tool.completed":
                call = event["call_id"]
                if state != "running" or not isinstance(call, str) or call not in outstanding:
                    return None, spent
                outstanding.remove(call)
            elif kind in {"run.accepted", "run.running", "run.success"}:
                return None, spent
    return (cuts if state == "success" else None), spent


def _split_parts(parts: Sequence[dict[str, Any]], ends: Sequence[int]) -> list[TranscriptSegment]:
    output: list[TranscriptSegment] = []
    index = part_offset = position = 0
    for end in ends:
        segment: list[dict[str, Any]] = []
        while position < end:
            part = parts[index]
            if part["kind"] != "text":
                segment.append(part)
                index += 1
                continue
            value = part["text"]
            count = min(len(value) - part_offset, end - position)
            segment.append({**part, "text": value[part_offset : part_offset + count]})
            position += count
            part_offset += count
            if part_offset == len(value):
                index += 1
                part_offset = 0
        output.append({
            "text": "".join(part["text"] for part in segment if part["kind"] == "text"),
            "parts": segment,
        })
    # A rich part on a text boundary belongs to the following segment.
    # Preserve trailing rich parts even when the final text suffix is empty.
    output[-1]["parts"].extend(parts[index:])
    return output


__all__ = ["TranscriptSegment", "transcript_segments"]
