"""Read one saved invocation within an already authorized exact session boundary."""
from __future__ import annotations
import json
from openagent_core.contracts import ExecutionContext, SessionRef


def read_tool_invocation(store, reference: SessionRef, tool_call_id: str, context: ExecutionContext):
    # The host still authorizes session.read immediately before/after this read.
    # Store boundaries independently refuse cross-tenant/agent/session selectors.
    if reference.session_id != context.session_id:
        raise PermissionError("Invocation must belong to the verified session")
    if not isinstance(tool_call_id, str) or not 1 <= len(tool_call_id) <= 512 or any(ord(c) < 32 for c in tool_call_id):
        raise ValueError("Invalid tool invocation identifier")
    store._require_local_session(reference, context)
    rows = store.connection.execute('''SELECT t.* FROM tool_invocations t
        JOIN session_runs r ON r.id=t.session_run_id
        WHERE t.tenant_id=? AND t.session_id=? AND r.agent_id=?
          AND t.root_kind='session' AND t.root_id=? AND t.visibility<>'quarantined'
          AND (t.id=? OR t.tool_call_id=? OR json_extract(t.raw_envelope_json,'$.binding.provider_call_id')=?)
        LIMIT 2''', (reference.tenant_id, reference.session_id, reference.agent_id,
                    reference.session_id, tool_call_id, tool_call_id, tool_call_id)).fetchall()
    if not rows:
        raise LookupError("Tool invocation not found in this session")
    if len(rows) != 1:
        raise ValueError("Tool invocation identifier is ambiguous; use the invocation id")
    row = rows[0]
    store._require_run_not_quarantined(row['session_run_id'])
    envelope = json.loads(row['raw_envelope_json'])
    return {"id": row['id'], "tool_call_id": row['tool_call_id'], "run_id": row['session_run_id'],
        "session_id": reference.session_id, "status": row['status'],
        "binding": envelope.get('binding', {}), "arguments": json.loads(row['args_json'] or '{}'),
        "result": json.loads(row['result_json']) if row['result_json'] is not None else None,
        "error": json.loads(row['error_json']) if row['error_json'] is not None else None}
