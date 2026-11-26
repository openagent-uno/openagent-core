"""Recover missing normalized message parts from the exact admitted run."""

from __future__ import annotations

import json
from pathlib import Path

from .artifact_document_history import visible_ids
from .artifacts import _attachment_ref, attachment_kind, public_attachment_ref, safe_attachment_filename
from .operational.access import resources_are_visible


def _admitted_parts(row, max_parts):
    from openagent_core.artifacts import canonical_artifact_id
    from openagent_core.stream.content_parts import parse_response_content

    text = row["text"] or ""
    source = json.loads(row["source"] or "null")
    selected = None
    parts = None
    if row["role"] == "user":
        refs = source
    elif (isinstance(source, dict) and source.get("kind") == "done" and not source.get("errored")
          and not source.get("privacy_redacted")
          and source.get("text") == text):
        refs = source.get("attachments")  # Older terminal frames used a flat list.
        if "final_answer" in source:
            # A modern frame can include input previews or tool media alongside
            # the attachments the assistant actually selected for delivery.
            final = source["final_answer"]
            if not isinstance(final, dict) or not isinstance(final.get("attachments"), list):
                return []
            selected = {ref["artifact_id"] for ref in final["attachments"][:max_parts]
                        if isinstance(ref, dict) and canonical_artifact_id(ref.get("artifact_id"))}
            if not selected:
                return []
            if refs is None:
                refs = final["attachments"]
        parts = source.get("parts")
        if parts is not None:
            if not isinstance(parts, list) or len(parts) > max_parts or any(not isinstance(part, dict) for part in parts):
                return []
            # Mixed content order is reliable only when these are exactly the
            # visible message's text segments. Never restore edited or redacted text.
            segments = [part.get("text") for part in parts if part.get("kind") == "text"]
            if not all(isinstance(value, str) for value in segments) or "".join(segments).strip() != text.strip():
                return []
    else:
        return []
    if parts is None:
        if not isinstance(refs, list) or not refs or len(refs) > max_parts:
            return []
        parts = ([{"kind": "text", "text": text}] if text else []) + [
            {"kind": "attachment", "attachment": ref} for ref in refs]
    if parse_response_content(text, allow_inline_ui=True).text != text.strip():
        return []
    public = []
    seen = set()
    for part in parts:
        if part.get("kind") == "text":
            public.append({"kind": "text", "text": part["text"]})
        elif part.get("kind") == "attachment" and isinstance(part.get("attachment"), dict):
            ref = part["attachment"]
            aid = ref.get("artifact_id")
            if canonical_artifact_id(aid) and aid not in seen and (selected is None or aid in selected):
                public.append({"kind": "attachment", "attachment": ref})
                seen.add(aid)
    return public if seen else []


async def runtime_parts_for_page(conn, message_ids, access, *, max_parts):
    """Fill a bounded page of missing parts without creating new artifact links."""
    has_views = await (await conn.execute(
        "SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ui_message_links'"
    )).fetchone()
    preserve_views = """AND NOT EXISTS (SELECT 1 FROM ui_message_links v WHERE v.tenant_id=m.tenant_id
                           AND v.session_id=m.session_id AND v.message_id=m.id)""" if has_views else ""
    rows = await (await conn.execute(f"""
        SELECT m.id, m.role, m.text, m.created_at_ms, s.id AS resource_id, 'session' AS resource_type,
               s.tenant_id, s.owner_principal_id, s.visibility, s.acl_version,
               CASE m.role WHEN 'user' THEN json_extract(r.metadata_json,'$.attachments')
                    ELSE (SELECT CASE WHEN e.sensitivity='normal' THEN e.metadata_json END
                            FROM domain_events e
                           WHERE e.run_id=r.id AND e.tenant_id=r.tenant_id AND e.session_id=r.session_id
                             AND e.resource_type='run' AND e.resource_id=r.id AND e.event_type='run.stream'
                             AND (json_extract(e.metadata_json,'$.kind')='done'
                                  OR json_extract(e.metadata_json,'$.privacy_redacted')=1)
                           ORDER BY e.sequence DESC LIMIT 1) END AS source
          FROM json_each(?) page CROSS JOIN session_messages m ON m.id=page.value
          JOIN session_runs r ON r.id=m.run_id AND r.tenant_id=m.tenant_id AND r.session_id=m.session_id
          JOIN sessions_v2 s ON s.id=m.session_id AND s.tenant_id=m.tenant_id AND s.agent_id=r.agent_id
         WHERE m.tenant_id=? AND s.deleted_at_ms IS NULL AND m.status='complete' AND m.visibility='user_visible'
           AND m.source_version=1 AND m.legacy_inferred=0 AND m.raw_envelope_json ->> '$.from_history' IS NOT 1
           AND m.raw_envelope_json ->> '$.privacy_redacted' IS NOT 1 AND r.metadata_json ->> '$.runtime_contract'=1
           AND NOT EXISTS (SELECT 1 FROM session_message_parts p WHERE p.message_id=m.id)
           AND NOT EXISTS (SELECT 1 FROM artifact_links l WHERE l.tenant_id=m.tenant_id
                           AND l.resource_type='message' AND l.resource_id=m.id)
           {preserve_views}
           AND ((m.role='user' AND m.ordinal=0 AND m.text=json_extract(r.input_json,'$')
                 AND m.author_principal_id=r.metadata_json ->> '$.execution_context.author'
                 AND json_type(r.metadata_json,'$.attachments[0]')='object')
             OR (m.role='assistant' AND m.author_kind='agent' AND m.tool_call_id IS NULL AND r.status='success'
                 AND json_type(r.output_json)='text' AND m.text=json_extract(r.output_json,'$')
                 AND m.id=(SELECT last.id FROM session_messages last WHERE last.run_id=m.run_id
                             AND last.tenant_id=m.tenant_id AND last.session_id=m.session_id
                             AND last.role='assistant' AND last.tool_call_id IS NULL
                             AND last.raw_envelope_json ->> '$.from_history' IS NOT 1
                           ORDER BY last.ordinal DESC LIMIT 1)))
        """, (json.dumps(message_ids), access.tenant_id))).fetchall()
    by_session = {row["resource_id"]: row for row in rows}
    allowed = await resources_are_visible(conn, list(by_session.values()), access)
    sessions = {sid for sid, visible in zip(by_session, allowed) if visible}
    parts = {}
    wanted = []
    for row in rows:
        if row["resource_id"] not in sessions:
            continue
        sequence = _admitted_parts(row, max_parts)
        for position, part in enumerate(sequence):
            if part["kind"] == "attachment":
                ref = part["attachment"]
                wanted.append([row["id"], position, row["resource_id"], row["role"],
                               ref["artifact_id"], ref.get("artifact_link_id"), row["created_at_ms"]])
                parts[row["id"]] = sequence
    if not wanted:
        return {}

    # A journal reference is a lookup hint. Select one still-linked object in
    # the exact session and use only its currently authorized local display name.
    links = await (await conn.execute("""
        WITH matches AS (
            SELECT w.value ->> '$[0]' AS message, w.value ->> '$[1]' AS position,
                   a.id, a.tenant_id, a.metadata_json, a.kind, a.mime, a.size_bytes, a.sha256,
                   l.id AS link_id,
                   min(coalesce(l.display_name,a.id)) OVER (PARTITION BY w.key) AS first_name,
                   max(coalesce(l.display_name,a.id)) OVER (PARTITION BY w.key) AS last_name,
                   row_number() OVER (PARTITION BY w.key ORDER BY l.created_at_ms,l.id) AS rank
              FROM json_each(?) w CROSS JOIN artifacts a ON a.id=w.value ->> '$[4]' AND a.tenant_id=?
              JOIN artifact_links l INDEXED BY idx_artifact_links_artifact ON l.tenant_id=a.tenant_id AND l.artifact_id=a.id
               AND l.resource_type='session' AND l.resource_id=w.value ->> '$[2]'
               AND (l.id=w.value ->> '$[5]' OR (w.value ->> '$[5]' IS NULL AND l.created_at_ms<=w.value ->> '$[6]'))
               AND (l.relation='input_attachment' OR (l.relation='output_attachment' AND w.value ->> '$[3]'='assistant'))
             WHERE a.deleted_at_ms IS NULL AND a.storage_state='available')
        SELECT * FROM matches WHERE rank=1
        """, (json.dumps(wanted), access.tenant_id))).fetchall()
    visible = await visible_ids(conn, access.tenant_id, links, getattr(access, "projection", None))
    current = {}
    for link in links:
        if link["id"] in visible:
            filename = link["first_name"] if link["first_name"] == link["last_name"] else link["id"]
            ref = public_attachment_ref(_attachment_ref(link, path=Path(),
                filename=safe_attachment_filename(filename),
                kind=attachment_kind(link["mime"], link["kind"]), link_id=link["link_id"]))
            current[link["message"], link["position"]] = {"kind": "attachment", "attachment": ref}
    return {mid: [part if part["kind"] == "text" else current[mid, pos]
                  for pos, part in enumerate(sequence) if part["kind"] == "text" or (mid, pos) in current]
            for mid, sequence in parts.items()}
