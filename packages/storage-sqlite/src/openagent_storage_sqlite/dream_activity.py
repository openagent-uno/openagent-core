"""A bounded projection of authorized canonical runtime turns for document dreams."""

import json
from dataclasses import replace

from openagent_core.dream_evidence import (
    CANDIDATE_RUNS,
    EVENT_KINDS,
    EVENTS_PER_RUN,
    MESSAGE_CHARS,
    MESSAGES_PER_RUN,
)


def check_dream_session(self, ref, context):
    self._require_local_session(ref, context)
    principals = {context.authority, context.initiator, *context.audience}
    if len(principals) > 18:
        raise PermissionError("Dream audience exceeds its bound")
    for principal in principals:
        predicate, values = self._personal_visibility(replace(context, initiator=principal), "view")
        if (
            self.connection.execute(
                "SELECT 1 FROM sessions_v2 s WHERE s.id=? AND " + predicate, (ref.session_id, *values)
            ).fetchone()
            is None
        ):
            raise PermissionError("Dream source session unavailable")


def dream_evidence_receipt(self, ident):
    row = self.connection.execute(
        "SELECT metadata_json FROM domain_events WHERE run_id=? AND event_type='run.dream.evidence' LIMIT 1",
        (ident,),
    ).fetchone()
    return json.loads(row[0]) if row else None


def dream_session_activity(self, ref, context, *, since_ms, limit, run_ids=None):
    from openagent_core.contracts import canonical_json

    if type(limit) is not int or not 1 <= limit <= 8 or type(since_ms) is not int:
        raise ValueError("Invalid activity query")
    check_dream_session(self, ref, context)
    members = {p.key for p in (context.authority, context.initiator, *context.audience)}
    db = self.connection
    params = [ref.tenant_id, ref.session_id, ref.agent_id]
    selected = ""
    if run_ids is not None:
        if not isinstance(run_ids, tuple) or len(run_ids) > limit:
            raise ValueError("Invalid activity selection")
        if not run_ids:
            return []
        selected = " AND id IN (" + ",".join("?" for _ in run_ids) + ")"
        params.extend(run_ids)
    rows = db.execute(
        f"""
        WITH recent AS MATERIALIZED (
            SELECT id, ordinal, status, finished_at_ms, metadata_json FROM session_runs
            WHERE tenant_id=? AND session_id=? AND agent_id=? {selected} ORDER BY ordinal DESC LIMIT ?
        )
        SELECT id,status,finished_at_ms,json_extract(metadata_json,'$.execution_context.authority') authority,
               json_extract(metadata_json,'$.execution_context.initiator') initiator,
               json_extract(metadata_json,'$.execution_context.audience') audience
        FROM recent WHERE finished_at_ms>=? AND length(CAST(id AS BLOB))<=200
        AND json_type(metadata_json,'$.execution_context.audience')='array'
        AND json_array_length(metadata_json,'$.execution_context.audience') BETWEEN 1 AND 64 ORDER BY ordinal DESC
        """,
        (*params, CANDIDATE_RUNS, since_ms),
    ).fetchall()
    results = []
    for r in rows:
        if not r["authority"] or not r["initiator"]:
            continue
        audience = json.loads(r["audience"])
        if not members.issubset({r["authority"], r["initiator"], *(canonical_json(p) for p in audience)}):
            continue
        messages = db.execute(
            """
            SELECT role,substr(text,1,?) excerpt FROM (
                SELECT role,status,text,visibility FROM session_messages WHERE run_id=?
                ORDER BY ordinal DESC LIMIT ?) WHERE role IN ('user','assistant') AND status='complete'
                AND visibility='user_visible' AND text IS NOT NULL
            """,
            (MESSAGE_CHARS, r["id"], MESSAGES_PER_RUN),
        ).fetchall()
        events = db.execute(
            """
            SELECT event_type kind, CASE WHEN event_type='tool.completed'
                AND json_extract(metadata_json,'$.status') IN ('success','error')
                THEN json_extract(metadata_json,'$.status') END AS outcome
            FROM (SELECT event_type,metadata_json,sensitivity FROM domain_events
                  WHERE run_id=? ORDER BY sequence DESC LIMIT ?) WHERE sensitivity='normal'
            """,
            (r["id"], EVENTS_PER_RUN),
        ).fetchall()
        results.append(
            {
                "run_id": r["id"],
                "status": r["status"],
                "finished_at_ms": r["finished_at_ms"],
                "messages": [dict(m) for m in reversed(messages)],
                "events": [dict(e) for e in reversed(events) if e["kind"] in EVENT_KINDS],
            }
        )
        if len(results) == limit:
            break
    return results
