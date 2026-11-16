"""A conversation's goal: work continues on its own until the agent achieves it.

``/goal <objective>`` (or the session API) gives one conversation an objective.
While the goal is active, every turn in that conversation sees it in its system
prompt, and when the model would end its turn the agent loop continues in the
same session with a short reminder instead of stopping. The agent ends the goal
by writing ``[goal: achieved] <outcome>`` on its own line in a reply, or hands
control back with ``[goal: blocked] <reason>`` when only the user can unblock
it. A marker needs no tool, so it works in every host and capability set. Each automatic continuation consumes one of the
goal's ``max_continuations``; when they run out the goal pauses itself, so an
unreachable goal can never loop forever. The user can pause, resume, edit or
clear it at any time, and the stop button still cancels the running turn.

State lives in the agent database (table ``session_goals``), one row per
session. Reads and writes use a dedicated connection, so a goal changed by a
command or the API is visible to the running turn at once.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Optional

ACTIVE = "active"
PAUSED = "paused"
ACHIEVED = "achieved"
STATUSES = (ACTIVE, PAUSED, ACHIEVED)

DEFAULT_MAX_CONTINUATIONS = 20
MAX_MAX_CONTINUATIONS = 500
MAX_OBJECTIVE_CHARS = 4000
MAX_NOTE_CHARS = 2000

_DDL = (
    "CREATE TABLE IF NOT EXISTS session_goals ("
    "session_id TEXT PRIMARY KEY, "
    "objective TEXT NOT NULL, "
    "status TEXT NOT NULL CHECK (status IN ('active','paused','achieved')), "
    "max_continuations INTEGER NOT NULL CHECK (max_continuations >= 0), "
    "continuations INTEGER NOT NULL DEFAULT 0 CHECK (continuations >= 0), "
    "note TEXT, "
    "outcome TEXT, "
    "paused_reason TEXT, "
    "created_at_ms INTEGER NOT NULL, "
    "updated_at_ms INTEGER NOT NULL, "
    "achieved_at_ms INTEGER)"
)


@dataclass(frozen=True)
class SessionGoal:
    session_id: str
    objective: str
    status: str
    max_continuations: int
    continuations: int
    note: Optional[str]
    outcome: Optional[str]
    paused_reason: Optional[str]
    created_at_ms: int
    updated_at_ms: int
    achieved_at_ms: Optional[int]

    @property
    def remaining(self) -> int:
        return max(0, self.max_continuations - self.continuations)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "objective": self.objective,
            "status": self.status,
            "max_continuations": self.max_continuations,
            "continuations": self.continuations,
            "remaining": self.remaining,
            "note": self.note,
            "outcome": self.outcome,
            "paused_reason": self.paused_reason,
            "created_at_ms": self.created_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "achieved_at_ms": self.achieved_at_ms,
        }


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clip(text: Optional[str], limit: int) -> Optional[str]:
    if text is None:
        return None
    text = str(text).strip()
    return text[:limit] if text else None


def _objective(text: Optional[str]) -> str:
    """The objective as it will be stored; too long is an error, never a silent cut."""
    value = (text or "").strip()
    if len(value) > MAX_OBJECTIVE_CHARS:
        raise ValueError(
            f"The objective is {len(value)} characters long; the limit is {MAX_OBJECTIVE_CHARS}. "
            "Shorten it and try again.")
    return value


def _row(row: Any) -> Optional[SessionGoal]:
    if row is None:
        return None
    return SessionGoal(*tuple(row))


_COLUMNS = ("session_id, objective, status, max_continuations, continuations, note, outcome, "
            "paused_reason, created_at_ms, updated_at_ms, achieved_at_ms")


class GoalStore:
    """Session goals in the agent database ``db`` (a ``MemoryDB``)."""

    def __init__(self, db: Any) -> None:
        if db is None:
            raise ValueError("a memory database is required")
        self.db = db

    async def _run(self, fn):
        from openagent_core.memory.artifacts import _connection

        async with _connection(self.db) as conn:
            await conn.execute(_DDL)
            try:
                result = await fn(conn)
                await conn.commit()
                return result
            except BaseException:
                await conn.rollback()
                raise

    @staticmethod
    async def _fetch(conn: Any, session_id: str) -> Optional[SessionGoal]:
        cur = await conn.execute(f"SELECT {_COLUMNS} FROM session_goals WHERE session_id=?", (session_id,))
        return _row(await cur.fetchone())

    async def get(self, session_id: str) -> Optional[SessionGoal]:
        if not session_id:
            return None
        return await self._run(lambda conn: self._fetch(conn, session_id))

    async def set(self, session_id: str, objective: str, *,
                  max_continuations: Optional[int] = None) -> SessionGoal:
        """Give the session a new active goal, replacing any previous one."""
        objective = _objective(objective)
        if not session_id or not objective:
            raise ValueError("a session and a non-empty objective are required")
        cap = DEFAULT_MAX_CONTINUATIONS if max_continuations is None else int(max_continuations)
        if not 0 <= cap <= MAX_MAX_CONTINUATIONS:
            raise ValueError(f"max_continuations must be between 0 and {MAX_MAX_CONTINUATIONS}")
        now = _now_ms()

        async def write(conn):
            await conn.execute(
                "INSERT INTO session_goals (session_id, objective, status, max_continuations, continuations, "
                "note, outcome, paused_reason, created_at_ms, updated_at_ms, achieved_at_ms) "
                "VALUES (?, ?, 'active', ?, 0, NULL, NULL, NULL, ?, ?, NULL) "
                "ON CONFLICT(session_id) DO UPDATE SET objective=excluded.objective, status='active', "
                "max_continuations=excluded.max_continuations, continuations=0, note=NULL, outcome=NULL, "
                "paused_reason=NULL, created_at_ms=excluded.created_at_ms, updated_at_ms=excluded.updated_at_ms, "
                "achieved_at_ms=NULL",
                (session_id, objective, cap, now, now))
            return await self._fetch(conn, session_id)

        return await self._run(write)

    async def update(self, session_id: str, *, objective: Optional[str] = None,
                     max_continuations: Optional[int] = None) -> Optional[SessionGoal]:
        """Edit an existing goal without resetting its progress."""
        async def write(conn):
            goal = await self._fetch(conn, session_id)
            if goal is None:
                return None
            new_objective = _objective(objective) if objective is not None else goal.objective
            if not new_objective:
                raise ValueError("the objective cannot be empty")
            cap = goal.max_continuations if max_continuations is None else int(max_continuations)
            if not 0 <= cap <= MAX_MAX_CONTINUATIONS:
                raise ValueError(f"max_continuations must be between 0 and {MAX_MAX_CONTINUATIONS}")
            await conn.execute(
                "UPDATE session_goals SET objective=?, max_continuations=?, updated_at_ms=? WHERE session_id=?",
                (new_objective, cap, _now_ms(), session_id))
            return await self._fetch(conn, session_id)

        return await self._run(write)

    async def _transition(self, session_id: str, status: str, *, allowed_from: tuple[str, ...],
                          outcome: Optional[str] = None, reason: Optional[str] = None,
                          reset_budget: bool = False) -> Optional[SessionGoal]:
        async def write(conn):
            goal = await self._fetch(conn, session_id)
            if goal is None or goal.status not in allowed_from:
                return goal
            now = _now_ms()
            await conn.execute(
                "UPDATE session_goals SET status=?, outcome=COALESCE(?, outcome), paused_reason=?, "
                "continuations=CASE WHEN ? THEN 0 ELSE continuations END, "
                "achieved_at_ms=CASE WHEN ?='achieved' THEN ? ELSE achieved_at_ms END, updated_at_ms=? "
                "WHERE session_id=?",
                (status, _clip(outcome, MAX_NOTE_CHARS), _clip(reason, MAX_NOTE_CHARS),
                 1 if reset_budget else 0, status, now, now, session_id))
            return await self._fetch(conn, session_id)

        return await self._run(write)

    async def pause(self, session_id: str, reason: Optional[str] = None) -> Optional[SessionGoal]:
        return await self._transition(session_id, PAUSED, allowed_from=(ACTIVE,), reason=reason)

    async def resume(self, session_id: str) -> Optional[SessionGoal]:
        """Reactivate a paused goal with a fresh continuation budget."""
        return await self._transition(session_id, ACTIVE, allowed_from=(PAUSED,), reset_budget=True)

    async def complete(self, session_id: str, outcome: Optional[str] = None) -> Optional[SessionGoal]:
        return await self._transition(session_id, ACHIEVED, allowed_from=(ACTIVE, PAUSED), outcome=outcome)

    async def note(self, session_id: str, note: str) -> Optional[SessionGoal]:
        async def write(conn):
            await conn.execute("UPDATE session_goals SET note=?, updated_at_ms=? WHERE session_id=?",
                               (_clip(note, MAX_NOTE_CHARS), _now_ms(), session_id))
            return await self._fetch(conn, session_id)

        return await self._run(write)

    async def clear(self, session_id: str) -> bool:
        async def write(conn):
            cur = await conn.execute("DELETE FROM session_goals WHERE session_id=?", (session_id,))
            return cur.rowcount > 0

        return await self._run(write)

    async def interrupted_by_restart(self) -> list[tuple[SessionGoal, str, bool]]:
        """Active goals whose latest run a runtime restart interrupted.

        Each entry is ``(goal, interrupted_run_id, was_restart_resume)``: the
        last flag says that the interrupted run was itself a resume after an
        earlier restart.
        """
        async def read(conn):
            cur = await conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_runs'")
            if await cur.fetchone() is None:
                return []
            cur = await conn.execute(f"SELECT {_COLUMNS} FROM session_goals WHERE status='active'")
            goals = [_row(row) for row in await cur.fetchall()]
            found = []
            for goal in goals:
                cur = await conn.execute(
                    "SELECT id, status, output_json, idempotency_key FROM session_runs "
                    "WHERE session_id=? ORDER BY ordinal DESC, created_at_ms DESC LIMIT 1", (goal.session_id,))
                run = await cur.fetchone()
                if run is None or run[1] != "interrupted":
                    continue
                try:
                    reason = (json.loads(run[2]) if run[2] else {}).get("reason")
                except (TypeError, ValueError, AttributeError):
                    reason = None
                if reason == "runtime_restarted":
                    found.append((goal, str(run[0]), str(run[3] or "").startswith(RESTART_RUN_PREFIX)))
            return found

        return await self._run(read)

    async def consume_continuation(self, session_id: str) -> Optional[SessionGoal]:
        """Take one automatic continuation from an active goal.

        Returns the goal after the increment, or ``None`` when the session has
        no active goal. When the budget is already spent the goal pauses itself
        and the returned goal is paused, so the caller stops.
        """
        async def write(conn):
            goal = await self._fetch(conn, session_id)
            if goal is None or goal.status != ACTIVE:
                return None
            now = _now_ms()
            if goal.continuations >= goal.max_continuations:
                await conn.execute(
                    "UPDATE session_goals SET status='paused', paused_reason=?, updated_at_ms=? "
                    "WHERE session_id=? AND status='active'",
                    (f"Reached the limit of {goal.max_continuations} automatic continuations.", now, session_id))
                return await self._fetch(conn, session_id)
            await conn.execute(
                "UPDATE session_goals SET continuations=continuations+1, updated_at_ms=? "
                "WHERE session_id=? AND status='active'", (now, session_id))
            return await self._fetch(conn, session_id)

        return await self._run(write)


def system_block(goal: Optional[SessionGoal]) -> str:
    """The stable prompt block for a session's goal (unchanged while it is).

    An active goal drives the work. A paused goal stays the standing context
    of the conversation: the agent still knows its role and rules when the
    user writes, but nothing continues on its own. An achieved goal adds
    nothing.
    """
    if goal is None:
        return ""
    if goal.status == PAUSED:
        reason = f" ({goal.paused_reason})" if goal.paused_reason else ""
        return (
            "<session-goal>\n"
            f"This conversation has a goal set by the user. It is paused{reason}:\n"
            f"{goal.objective}\n\n"
            "It is still the standing context of this conversation: follow it when you "
            "answer or work here. It does not continue on its own while paused; the user "
            "or a schedule resumes it. Do not write `[goal: ...]` markers while it is paused.\n"
            "</session-goal>"
        )
    if goal.status != ACTIVE:
        return ""
    return (
        "<session-goal>\n"
        "This conversation has an active goal set by the user:\n"
        f"{goal.objective}\n\n"
        "Work toward it on your own across turns: when you would stop, the conversation "
        "continues automatically until the goal is done. When it is fully achieved and "
        "verified, end your reply with a line `[goal: achieved] <one-line outcome>`. If only "
        "the user can unblock you (a decision, access, credentials), end your reply with a "
        "line `[goal: blocked] <what you need>` instead of looping. Never mark the goal "
        "achieved without evidence.\n"
        "</session-goal>"
    )


def continuation_prompt(goal: SessionGoal) -> str:
    """The reminder that drives the next automatic continuation."""
    note = f"\nLast progress note: {goal.note}" if goal.note else ""
    return (
        "<system-reminder>\n"
        f"The session goal is still open (automatic continuation {goal.continuations} of "
        f"{goal.max_continuations}):\n{goal.objective}{note}\n\n"
        "Continue working toward it now and take the next concrete step. If it is fully "
        "achieved, end your reply with `[goal: achieved] <outcome>`. If you are blocked on "
        "something only the user can provide, end your reply with `[goal: blocked] <reason>`.\n"
        "</system-reminder>"
    )


RESTART_RUN_PREFIX = "goal-restart:"


def restart_prompt(goal: SessionGoal) -> str:
    """The reminder that restarts work on a goal after a runtime restart."""
    note = f"\nLast progress note: {goal.note}" if goal.note else ""
    return (
        "<system-reminder>\n"
        "The runtime restarted while you were working, and your previous turn was interrupted. "
        "Commands and tool calls that were running then did not report back: they may have "
        "finished, failed or never run. Check the actual state before you repeat any of them, "
        "and do not rerun the step that was in progress if it may have caused the restart "
        "(for example a command that used too much memory).\n\n"
        f"The session goal is still open (automatic continuation {goal.continuations} of "
        f"{goal.max_continuations}):\n{goal.objective}{note}\n\n"
        "Continue working toward it now. If it is fully achieved, end your reply with "
        "`[goal: achieved] <outcome>`. If you are blocked on something only the user can "
        "provide, end your reply with `[goal: blocked] <reason>`.\n"
        "</system-reminder>"
    )


async def resume_after_restart(db: Any, *, authority: Any, submit: Any) -> list[dict[str, Any]]:
    """Restart work on the goals a runtime restart interrupted.

    A host calls this once after it started. ``authority(session_id)`` returns
    the durable delegation the goal's owner gave it (an ``ExecutionContext``
    for deferred work) or ``None``; ``submit(request, context)`` admits the
    run. A goal resumes at most once per interrupted run and uses one
    continuation. Without a delegation, or when the interrupted run was
    already a resume (a restart loop), the goal pauses with the reason, so it
    never reads as active while nothing works on it. Nothing here raises.
    """
    import uuid

    from openagent_core.contracts import RunRequest

    store = GoalStore(db)
    results: list[dict[str, Any]] = []
    try:
        interrupted = await store.interrupted_by_restart()
    except Exception as exc:  # noqa: BLE001 - recovery must never fail a start
        return [{"action": "failed", "error": type(exc).__name__}]
    for goal, run_id, was_resume in interrupted:
        session_id = goal.session_id
        try:
            if was_resume:
                await store.pause(session_id, "The runtime restarted twice while working on this goal. "
                                              "Resume it or send a message to continue.")
                results.append({"session_id": session_id, "action": "paused", "reason": "repeated_restart"})
                continue
            try:
                context = await authority(session_id)
            except Exception:  # noqa: BLE001 - no usable delegation
                context = None
            if context is None:
                await store.pause(session_id, "The runtime restarted while working on this goal. "
                                              "Resume it or send a message to continue.")
                results.append({"session_id": session_id, "action": "paused", "reason": "no_delegation"})
                continue
            current = await store.consume_continuation(session_id)
            if current is None or current.status != ACTIVE:
                results.append({"session_id": session_id, "action": "paused", "reason": "budget"})
                continue
            new_run_id = str(uuid.uuid4())
            await submit(RunRequest(new_run_id, session_id, RESTART_RUN_PREFIX + run_id,
                                    restart_prompt(current)), context)
            results.append({"session_id": session_id, "action": "resumed", "run_id": new_run_id})
        except Exception as exc:  # noqa: BLE001 - one goal must not block the others
            results.append({"session_id": session_id, "action": "failed", "error": type(exc).__name__})
    return results


_MARKER = re.compile(r"^[ \t>*_`-]*\[goal:\s*(achieved|blocked)\][ \t]*(.*)$", re.IGNORECASE | re.MULTILINE)


def parse_marker(text: Optional[str]) -> Optional[tuple[str, str]]:
    """The last ``[goal: achieved|blocked]`` line of a reply, as (kind, detail)."""
    if not text:
        return None
    found = None
    for match in _MARKER.finditer(text):
        found = (match.group(1).lower(), match.group(2).strip().strip("`*_").strip())
    return found


async def decorate_system(db: Any, session_id: Optional[str], system: str) -> str:
    """Append the active goal of ``session_id`` to a composed system prompt."""
    if db is None or not session_id:
        return system
    try:
        goal = await GoalStore(db).get(session_id)
    except Exception:  # noqa: BLE001 - a goal must never break a turn
        return system
    block = system_block(goal)
    return f"{system}\n\n{block}" if block else system


async def after_reply(db: Any, session_id: Optional[str], reply: Optional[str]) -> Optional[str]:
    """Decide what happens when a turn in a goal session would end.

    Applies a ``[goal: achieved]`` / ``[goal: blocked]`` marker in ``reply``
    and returns ``None`` (stop), or takes one automatic continuation and
    returns its reminder. Sessions without an active goal always stop.
    """
    if db is None or not session_id:
        return None
    try:
        store = GoalStore(db)
        goal = await store.get(session_id)
        if goal is None or goal.status != ACTIVE:
            return None
        marker = parse_marker(reply)
        if marker is not None:
            kind, detail = marker
            if kind == "achieved":
                await store.complete(session_id, detail or None)
            else:
                await store.pause(session_id, detail or "The agent needs input from the user.")
            return None
        goal = await store.consume_continuation(session_id)
    except Exception:  # noqa: BLE001 - a goal must never break a turn
        return None
    if goal is None or goal.status != ACTIVE:
        return None
    return continuation_prompt(goal)


def describe(goal: Optional[SessionGoal]) -> str:
    """A one-paragraph human summary used by ``/goal status``."""
    if goal is None:
        return "No goal for this conversation. Set one with /goal <objective>."
    lines = [f"Goal ({goal.status}): {goal.objective}",
             f"Automatic continuations: {goal.continuations} of {goal.max_continuations}"]
    if goal.note:
        lines.append(f"Last note: {goal.note}")
    if goal.status == PAUSED and goal.paused_reason:
        lines.append(f"Paused: {goal.paused_reason}")
    if goal.status == ACHIEVED and goal.outcome:
        lines.append(f"Outcome: {goal.outcome}")
    return "\n".join(lines)
