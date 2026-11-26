"""Attested presentation of text around canonical transcript tool rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

from openagent_core.memory.message_parts import (
    MAX_MESSAGE_PARTS,
    MAX_TEXT_PART_BYTES,
    canonical_parts_for_messages_on_connection,
)
from openagent_core.memory.operational.access import AccessContext, resource_is_visible
from openagent_core.stream.content_parts import parse_response_content


_MAX_PAGE_MESSAGES = 256
_MAX_TURN_MESSAGES = 512
_MAX_TURN_EVENTS = 16384
_MAX_TURN_TEXT_BYTES = 512 * 1024


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
    result: dict[str, TranscriptSegment] = {}
    for run in runs:
        run_id = run["id"]
        rows = await _turn_messages(conn, access.tenant_id, session_id, run_id, tables)
        if not _can_layout(rows):
            continue
        answer = rows[-1]
        public_text = parse_response_content(answer["text"], allow_inline_ui=True).text
        if not answer["part_count"] and (public_text != answer["text"] or answer["has_rich_links"]):
            continue
        cuts = await _attest_journal(
            conn, access.tenant_id, session_id, run_id,
            [row["tool_call_id"] for row in rows[:-1]], public_text, has_parts=bool(answer["part_count"]),
        )
        if cuts is None:
            continue
        parts = await _visible_parts(conn, access, answer, public_text, has_views="ui_views" in tables)
        if parts is None:
            continue
        for row, segment in zip(rows, _split_parts(parts, [*cuts, len(public_text)]), strict=True):
            if page[run_id].get(row["id"]) == row["role"]:
                result[row["id"]] = segment
    return result


async def _turn_messages(
    conn: Any, tenant_id: str, session_id: str, run_id: str, tables: set[str],
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
            "CASE WHEN m.role='assistant' THEN json_extract(m.raw_envelope_json,'$.privacy_redacted') END AS redacted, "
            f"CASE WHEN m.role='assistant' THEN {part_count} END AS part_count, "
            f"CASE WHEN m.role='assistant' THEN {part_bytes} END AS part_bytes, "
            "CASE WHEN m.role='assistant' THEN (EXISTS(SELECT 1 FROM artifact_links l "
            "WHERE l.tenant_id=m.tenant_id AND l.resource_type='message' AND l.resource_id=m.id)"
            f" {rich_views}) END AS has_rich_links "
            "FROM session_messages m "
            "WHERE m.run_id=? AND m.tenant_id=? AND m.session_id=? AND m.role IN ('assistant','tool') "
            "ORDER BY m.ordinal LIMIT ?",
            (_MAX_TURN_TEXT_BYTES, run_id, tenant_id, session_id, _MAX_TURN_MESSAGES + 1),
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


async def _visible_parts(
    conn: Any, access: AccessContext, answer: Any, text: str, *, has_views: bool,
) -> list[dict[str, Any]] | None:
    if not answer["part_count"]:
        return [{"kind": "text", "text": text}] if text else []
    hydrated = await canonical_parts_for_messages_on_connection(
        conn, [answer["id"]], access=access if has_views else None,
    )
    parts = hydrated.get(answer["id"], [])
    # A rich part omitted by current access checks makes the layout ambiguous.
    if len(parts) != answer["part_count"]:
        return None
    return parts if "".join(part["text"] for part in parts if part["kind"] == "text") == text else None


async def _attest_journal(
    conn: Any,
    tenant_id: str,
    session_id: str,
    run_id: str,
    tool_ids: Sequence[str],
    text: str,
    *, has_parts: bool,
) -> list[int] | None:
    # Only public deltas, the terminal frame and durable tool intent can
    # establish a boundary. Ignore diagnostics and unrelated lifecycle events.
    query = """
        SELECT event_type, sensitivity, json_extract(metadata_json,'$.privacy_redacted') AS redacted,
               CASE WHEN event_type='run.stream' THEN json_extract(metadata_json,'$.kind') END AS stream_kind,
               CASE WHEN ?=0 AND event_type='run.stream' AND json_extract(metadata_json,'$.kind')='done'
                    THEN CASE WHEN length(CAST(metadata_json AS BLOB))>? THEN 1
                         ELSE json_type(metadata_json,'$.attachments[0]') IS NOT NULL
                              OR json_type(metadata_json,'$.final_answer.attachments[0]') IS NOT NULL
                              OR EXISTS (SELECT 1 FROM json_each(metadata_json,'$.parts') p
                                         WHERE p.type='object' AND json_extract(p.value,'$.kind')='attachment')
                         END END AS unmaterialized_file,
               CASE WHEN event_type='run.stream' AND sensitivity='normal'
                    AND json_type(metadata_json,'$.text')='text'
                    AND length(CAST(json_extract(metadata_json,'$.text') AS BLOB))<=?
                    THEN json_extract(metadata_json,'$.text') END AS text,
               CASE WHEN event_type='tool.invoking' AND sensitivity='normal'
                    THEN json_extract(metadata_json,'$.call_id') END AS call_id
        FROM domain_events
        WHERE run_id=? AND tenant_id=? AND session_id=? AND resource_type='run' AND resource_id=?
          AND (event_type='tool.invoking' OR (event_type='run.stream' AND (
               json_extract(metadata_json,'$.kind') IN ('delta', 'done', 'error')
               OR json_extract(metadata_json,'$.privacy_redacted')=1)))
        ORDER BY sequence LIMIT ?
    """
    cuts: list[int] = []
    offset = 0
    verified_final = False
    parameters = (has_parts, MAX_TEXT_PART_BYTES, len(text.encode()), run_id, tenant_id, session_id, run_id, _MAX_TURN_EVENTS + 1)
    async with conn.execute(query, parameters) as cursor:
        count = 0
        async for event in cursor:
            count += 1
            if verified_final or count > _MAX_TURN_EVENTS or event["redacted"] or event["sensitivity"] != "normal":
                return None
            if event["event_type"] == "tool.invoking":
                if len(cuts) >= len(tool_ids) or event["call_id"] != tool_ids[len(cuts)]:
                    return None
                cuts.append(offset)
            elif event["stream_kind"] == "delta":
                chunk = event["text"]
                if chunk is None or not text.startswith(chunk, offset):
                    return None
                offset += len(chunk)
            elif event["stream_kind"] == "done":
                # A text-only segment would hide a native attachment on clients
                # rendering the interleaved page. Keep its canonical message.
                if event["unmaterialized_file"] or event["text"] != text or offset != len(text) or len(cuts) != len(tool_ids):
                    return None
                verified_final = True
            else:
                return None
    return cuts if verified_final else None


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
