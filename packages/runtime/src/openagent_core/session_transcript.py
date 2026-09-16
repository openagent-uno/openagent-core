"""Stable transcript expansion shared by embedded hosts and product clients."""
from __future__ import annotations

def _build_run_tool_index(
    run_tools: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Two lookup maps for ``runs[].tools[]`` entries used by the
    rehydration walk: by tool_call_id (precise — survives duplicate
    calls of the same name in one turn) and by tool_name (legacy
    fallback for rows that didn't persist tool_call_id).

    Each value is the runtime's native ``ToolExecution.to_dict()`` shape — the
    universal app consumes that directly via ``ToolInfo`` and derives
    phase locally from ``tool_call_error`` + ``result`` presence.
    """
    from openagent_core.models._tool_status import stored_tool_to_wire

    by_id: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    for t in run_tools or []:
        info = stored_tool_to_wire(t)
        if info is None:
            continue
        tid = t.get("tool_call_id") or t.get("tool_use_id") or t.get("id") or ""
        tn = t.get("tool_name") or t.get("name") or info.get("tool_name") or ""
        if tid:
            by_id[str(tid)] = info
        if tn and tn not in by_name:
            by_name[str(tn)] = info
    return by_id, by_name


def _attachments_from_images(imgs: list) -> list[dict]:
    out: list[dict] = []
    for img in imgs or []:
        if not isinstance(img, dict):
            continue
        fp = img.get("filepath") or img.get("url") or ""
        fn = (
            img.get("filename")
            or (fp.split("/")[-1] if "/" in fp else "")
            or "image.png"
        )
        if fp:
            out.append({"type": "image", "path": fp, "filename": fn})
    return out


def expand_run_messages(
    run: dict,
    *,
    timestamp: int,
    msg_counter: list[int],
    parent_model: str | None = None,
    parent_images: list | None = None,
    is_member_run: bool = False,
    parent_session_id: str | None = None,
    child_by_run_id: dict[str, str] | None = None,
    authorized_child_sessions: frozenset[str] = frozenset(),
) -> list[dict]:
    """Expand ONE runtime run dict into the flat ``ChatMessage`` shape the
    universal app expects, mirroring the live-wire event ordering.

    Recurses into ``member_responses`` whenever the leader's
    ``delegate_task_to_member`` tool call sits in the message stream —
    so a specialist's nested tool calls and its delegated content
    surface as their own tool chips + assistant messages with the
    specialist's model attribution. This is what the live path
    produces during streaming (specialist deltas via
    ``IntermediateRunContentEvent``, tool calls via the unified
    STATUS frame); the rehydration walk now matches that 1-for-1.

    The recursive design follows the runtime's stored shape exactly — a
    ``TeamRunOutput`` (with ``member_responses``) and a ``RunOutput``
    (without) reuse the same expansion because a member's tool calls
    live in its own ``runs[]``-equivalent ``tools`` list.
    """
    out: list[dict] = []
    run_status = str(run.get("status", "")).lower()
    if run_status in ("cancelled", "canceled"):
        return out

    # In-session compaction recap (vision §2). ``src.core.compaction``
    # folds the oldest runs into a single recap run tagged
    # ``metadata.compaction``. Surface it as a ``compaction`` message —
    # the same tool-style card the live ``session_compacted`` frame draws
    # — instead of letting the recap paragraph render as a bare assistant
    # bubble. The stats persisted alongside the recap let the reopened
    # transcript rebuild the identical card. This is a terminal shape for
    # the run: it has no user/tool/assistant messages worth expanding.
    meta = run.get("metadata")
    if isinstance(meta, dict) and meta.get("compaction"):
        msg_counter[0] += 1
        out.append({
            "id": f"run-msg-{msg_counter[0]}",
            "role": "compaction",
            "text": "",
            "timestamp": timestamp,
            "compaction": {
                "phase": "done",
                "folded_runs": int(meta.get("folded_runs") or 0),
                "kept_runs_count": int(meta.get("kept_runs_count") or 0),
                "summary_chars": int(
                    meta.get("summary_chars") or len(run.get("content") or "")
                ),
                "tokens_before": int(meta.get("tokens_before") or 0),
                "tokens_after": int(meta.get("tokens_after") or 0),
            },
        })
        return out

    run_tools = run.get("tools") or []
    run_tools_by_id, run_tools_by_name = _build_run_tool_index(run_tools)

    # Per-run model attribution. TeamRunOutput.to_dict() omits the
    # top-level ``model`` for the leader when it's a Team route — the
    # downstream UI then needs the entry_runtime_id we computed for the
    # synthetic ModelResponse. ``parent_model`` lets a recursing caller
    # override (used so member_responses inherit the team's leader
    # badge only when their own ``model`` is absent).
    run_model = run.get("model") or parent_model

    # Index member responses by member_id AND by stored index so we can
    # splice each delegation result inline. the runtime stores ``agent_id`` on
    # the nested RunOutput (the RuntimeAgent's name → url_safe_string),
    # which matches the ``member_id`` argument the leader passed to
    # ``delegate_task_to_member``. ``member_idx_by_run_id`` plus the
    # parallel ``agent_id`` map lets the splicing loop below find the
    # right member by id or by stored child_run_id without re-scanning.
    members_by_index: list[dict] = [
        mr for mr in (run.get("member_responses") or [])
        if isinstance(mr, dict)
    ]
    members_by_id: dict[str, int] = {}
    members_by_run_id: dict[str, int] = {}
    for idx, mr in enumerate(members_by_index):
        aid = str(mr.get("agent_id") or mr.get("team_id") or "")
        if aid:
            members_by_id.setdefault(aid, idx)
        rid = str(mr.get("run_id") or "")
        if rid:
            members_by_run_id.setdefault(rid, idx)

    # Track which member responses we've already spliced so we can
    # tack on any unmatched ones at the end (defensive against odd
    # rows where the leader's messages don't carry the delegate tool
    # results — happens with mid-turn cancellations).
    spliced_member_ids: set[int] = set()

    images_for_assistant = (
        (parent_images if parent_images is not None else run.get("images")) or []
    )

    for m in run.get("messages") or []:
        if not isinstance(m, dict):
            continue
        role = m.get("role", "user")
        content = m.get("content", "")
        if role == "system":
            continue
        if m.get("from_history"):
            continue
        # Member runs (nested under member_responses) carry the leader-
        # generated task prompt as their first user message. That's an
        # internal runtime artifact, not the human's input — surfacing it
        # would make the synthetic prompt show up in the chat IN PLACE
        # OF the user's actual message (which lives at the top-level
        # team run). Skip it; the specialist's assistant content and
        # tool calls still appear.
        if is_member_run and role == "user":
            continue

        # Tool-result message — render as a tool chip with the same
        # JSON envelope the live wire uses. Inline the nested member
        # run (if any) right after, so the specialist's own tool
        # calls + content appear in the correct slot in the
        # transcript.
        if role == "tool":
            msg_counter[0] += 1
            tcall_id = m.get("tool_call_id") or m.get("tool_use_id") or ""
            tname = m.get("name") or m.get("tool_name") or ""
            tool_info: dict | None = None
            if tcall_id:
                tool_info = run_tools_by_id.get(str(tcall_id))
            if tool_info is None and tname:
                tool_info = run_tools_by_name.get(str(tname))

            # If this tool result came from a delegate_task_to_member call,
            # resolve the matching member_responses entry. Match by
            # args.member_id → tools[].child_run_id → next un-spliced member.
            is_delegate = bool(
                tool_info
                and tool_info.get("tool_name") == "delegate_task_to_member"
            )
            member_idx: int | None = None
            if is_delegate:
                params = tool_info.get("tool_args") or {}
                member_id_arg = (
                    str(params.get("member_id") or "")
                    if isinstance(params, dict) else ""
                )
                if member_id_arg:
                    member_idx = members_by_id.get(member_id_arg)
                if member_idx is None and tcall_id:
                    matching_tool = next(
                        (t for t in run_tools
                         if str(t.get("tool_call_id")
                                or t.get("tool_use_id") or "") == str(tcall_id)),
                        None,
                    )
                    if matching_tool:
                        child_rid = str(matching_tool.get("child_run_id") or "")
                        if child_rid:
                            member_idx = members_by_run_id.get(child_rid)
                if member_idx is None:
                    member_idx = next(
                        (i for i in range(len(members_by_index))
                         if i not in spliced_member_ids),
                        None,
                    )

            # Did the member run as its OWN child session (a navigable row),
            # or nested in this (team) session (legacy)?
            #   1. Primary, deterministic: the parent delegate tool's
            #      ``child_run_id`` maps to a child session (built above from
            #      the child rows). Parallel-safe; independent of the team
            #      run_response object identity.
            #   2. Fallback (legacy rows): the member_response's session_id
            #      differs from the parent.
            child_member_sid = None
            crid = tool_info.get("child_run_id") if tool_info else None
            if crid and child_by_run_id:
                child_member_sid = child_by_run_id.get(str(crid))
            if child_member_sid is None and member_idx is not None:
                mr_sid = members_by_index[member_idx].get("session_id")
                if mr_sid and mr_sid != parent_session_id:
                    child_member_sid = mr_sid
            runs_as_child = bool(child_member_sid and child_member_sid != parent_session_id)

            # In child-session mode the chip MUST carry child_session_id so the
            # app renders a delegation card that deep-links into the member's
            # own session. Rehydration is the source of truth here — the stored
            # ToolExecution may not have captured the id (the live team-tool
            # object and the persisted one can differ), so backfill it from the
            # member_response. Copy the shared tool_info dict before mutating.
            if runs_as_child and child_member_sid in authorized_child_sessions and tool_info is not None and not tool_info.get("child_session_id"):
                tool_info = {**tool_info, "child_session_id": child_member_sid}

            entry: dict = {
                "id": f"run-msg-{msg_counter[0]}",
                "role": "tool",
                "text": content,
                "timestamp": timestamp,
            }
            if tool_info:
                if tool_info.get("child_session_id") not in authorized_child_sessions:
                    tool_info={k:v for k,v in tool_info.items() if k!="child_session_id"}
                entry["toolInfo"] = tool_info
            out.append(entry)

            if is_delegate and member_idx is not None:
                spliced_member_ids.add(member_idx)
                # Legacy nested mode: splice the member transcript inline so
                # the specialist's tool calls + content appear in-slot. In
                # child-session mode we DON'T splice — the member's transcript
                # lives in its own row; the parent shows only the card (the
                # member_responses entry exists purely to carry the link).
                if not runs_as_child:
                    out.extend(expand_run_messages(
                        members_by_index[member_idx],
                        timestamp=timestamp,
                        msg_counter=msg_counter,
                        parent_model=run_model,
                        is_member_run=True,
                        parent_session_id=parent_session_id,
                        child_by_run_id=child_by_run_id,
                        authorized_child_sessions=authorized_child_sessions,
                    ))
            continue

        # Empty assistant text without a tool-call carrier payload is
        # noise (an LLM that yielded zero deltas). Tool-call carriers
        # have empty content by design and are dropped here too — the
        # ``tool`` role message that follows carries the structured
        # ``toolInfo`` chip the UI renders.
        if not content and role == "assistant":
            continue

        msg_counter[0] += 1
        entry = {
            "id": f"run-msg-{msg_counter[0]}",
            "role": role,
            "text": content,
            "timestamp": timestamp,
        }
        # Per-message authorship: a human handle/display (so the app shows the
        # real sender instead of a generic "You", and multi-human sessions
        # attribute correctly) or an agent-self seed (the delegated task /
        # scheduled mission / workflow node prompt — rendered as a Mission
        # block). Absent on legacy rows → app falls back to the role label.
        if isinstance(m.get("author"), dict):
            entry["author"] = m["author"]
        if role == "assistant":
            # Specialist (member) runs carry their own model id; fall
            # back to the leader's badge only when the member row
            # lacks one (very old rows, or external-agent shims).
            if run_model:
                entry["model"] = run_model
            # Agent-attached files ride as ``[IMAGE:/p]`` / ``[VIDEO:/p]`` /
            # ``[VOICE:/p]`` / ``[FILE:/p]`` markers embedded in the STORED
            # assistant text — from the attachments MCP's ``send_file_to_user``
            # or auto-emitted for natively generated images. The live turn
            # strips them via ``parse_response_markers`` before the wire
            # ``response`` frame (stream/session.py); rehydration must do the
            # same, else a reopened — or, because the app reconciles the
            # transcript from this endpoint after every ``turn_complete``, even
            # a live — session shows the raw markers as literal text and loses
            # every non-image attachment (only ``run["images"]`` survived).
            atts: list[dict] = []
            if isinstance(content, str) and content:
                from openagent_core.media import parse_response_markers

                clean, marker_atts = parse_response_markers(content)
                entry["text"] = clean
                atts = [
                    {"type": a.type, "path": a.path, "filename": a.filename}
                    for a in marker_atts
                ]
            # Merge the runtime-image attachments, deduped by path so a
            # generated image present BOTH as a ``run["images"]`` entry and as an
            # inline ``[IMAGE:/p]`` marker isn't attached twice.
            seen_paths = {a["path"] for a in atts}
            for a in _attachments_from_images(images_for_assistant):
                if a["path"] not in seen_paths:
                    atts.append(a)
                    seen_paths.add(a["path"])
            if atts:
                entry["attachments"] = atts
            images_for_assistant = []  # only emit once per run
        out.append(entry)

    # Any member_responses that weren't spliced inline (e.g. the
    # leader's stored messages don't carry the delegate result, which
    # happens when the row was committed mid-turn) get appended after
    # the leader's content so the specialist's contribution still
    # appears in the transcript.
    for idx, mr in enumerate(members_by_index):
        if idx in spliced_member_ids:
            continue
        # A member that ran in its own child session is represented by its
        # card (the delegate chip), not by inlining its transcript here.
        mr_sid = mr.get("session_id")
        if mr_sid and mr_sid != parent_session_id:
            continue
        out.extend(expand_run_messages(
            mr,
            timestamp=timestamp,
            msg_counter=msg_counter,
            parent_model=run_model,
            is_member_run=True,
            parent_session_id=parent_session_id,
            child_by_run_id=child_by_run_id,
                        authorized_child_sessions=authorized_child_sessions,
        ))

    return out


