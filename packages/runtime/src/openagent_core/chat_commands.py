"""Chat commands shared by every OpenAgent host and client.

This module is the one definition of the slash commands (``/new``, ``/stop``,
``/status``...): their catalogue and their semantics. Hosts do not reimplement
them. A host authenticates the caller, builds a :class:`ChatCommandRequest` and
provides a :class:`ChatCommandHost`, the minimal port onto what only it can see:
its session model, its running turns and its queue.

Importing this module must stay free of heavy dependencies: channel bridges read
the catalogue from it in processes that do not start an engine.

Reset semantics (``new``, ``clear``, ``reset``):

* A running turn is stopped and queued input is dropped. Neither reaches the
  fresh conversation.
* The host rotates its session selector. The session model stays the host's.
* A host that retains history (``retains_history = True``) only has live
  resources released, so the previous conversation stays readable and
  resumable. Otherwise provider resume state and stored model history for the
  session are forgotten.
* Long-term memory, user memories and the vault are never touched.
* A shared conversation cannot be reset: one member must not erase the context
  of the others.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable

SESSION_SCOPE = "session"
INSTANCE_SCOPE = "instance"
COMMAND_SCOPES = frozenset({SESSION_SCOPE, INSTANCE_SCOPE})

COMPLETED = "completed"
UNSUPPORTED = "unsupported"
FAILED = "failed"

STOP_RUNS = "stop_runs"
ROTATE_SESSION = "rotate_session"

RESET_COMMANDS = frozenset({"new", "reset", "clear"})


@dataclass(frozen=True)
class ChatCommandSpec:
    name: str
    description: str
    help_text: str
    menu_visible: bool = True
    help_visible: bool = True
    # When set, the command's positional argument is chosen from a named,
    # server-known list rather than typed free-hand. Clients read this
    # (via ``GET /api/commands``) to render a picker instead of a text
    # field. Today the only source is ``"models"`` (enabled LLM models);
    # ``None`` means a plain free-text / no argument. Keeping it a string
    # rather than an enum lets new pickable sources appear without a
    # client redeploy.
    arg_source: str | None = None
    # ``session`` commands act on one conversation and work on every host.
    # ``instance`` commands administer a whole installation and exist only
    # where the host owns one.
    scope: str = SESSION_SCOPE


COMMAND_SPECS = (
    ChatCommandSpec("new", "Forget everything and start a fresh conversation", "wipe context, clear queue, start fresh"),
    ChatCommandSpec("reset", "Forget everything and start a fresh conversation", "wipe context, clear queue, start fresh", menu_visible=False, help_visible=False),
    ChatCommandSpec("clear", "Forget everything and start a fresh conversation", "wipe context, clear queue, start fresh"),
    ChatCommandSpec("stop", "Cancel the current operation (keeps context)", "cancel the current operation, keep conversation memory"),
    ChatCommandSpec("status", "Show agent status and queue", "show agent status and queue depth"),
    ChatCommandSpec("queue", "Show pending messages", "show pending messages", menu_visible=False),
    ChatCommandSpec("usage", "Show current usage and budget", "show current usage and budget", scope=INSTANCE_SCOPE),
    ChatCommandSpec("context", "Show context window usage for this conversation", "token/cost breakdown of the current context by section"),
    ChatCommandSpec("update", "Check for updates and install", "check for updates and install", scope=INSTANCE_SCOPE),
    ChatCommandSpec("restart", "Restart OpenAgent", "restart OpenAgent", scope=INSTANCE_SCOPE),
    ChatCommandSpec("help", "Show available commands", "show this help message"),
    ChatCommandSpec("compact", "Summarize & compress the conversation to free up context", "fold older turns into a recap to free context"),
    ChatCommandSpec("model", "Switch the model for this conversation", "list configured models or switch: /model <id>", arg_source="models"),
)

COMMAND_MAP = {spec.name: spec for spec in COMMAND_SPECS}
COMMANDS = tuple(spec.name for spec in COMMAND_SPECS)
BOT_COMMANDS = [(spec.name, spec.description) for spec in COMMAND_SPECS if spec.menu_visible]


def commands_for_scopes(scopes: Iterable[str] | None = None) -> tuple[ChatCommandSpec, ...]:
    """Return the catalogue a host with ``scopes`` offers; ``None`` means all."""
    if scopes is None:
        return COMMAND_SPECS
    selected = frozenset(scopes)
    if not selected <= COMMAND_SCOPES:
        raise ValueError("unknown chat command scope")
    return tuple(spec for spec in COMMAND_SPECS if spec.scope in selected)


def command_help_text(scopes: Iterable[str] | None = None) -> str:
    lines = ["Available commands:"]
    for spec in commands_for_scopes(scopes):
        if spec.help_visible:
            lines.append(f"• /{spec.name} — {spec.help_text}")
    return "\n".join(lines)


class ChatCommandUnavailable(Exception):
    """The host cannot perform this operation. Nothing was changed."""


@dataclass(frozen=True)
class SessionRef:
    """A host's reference to one conversation.

    ``session_id=None`` is the legacy whole-client scope of direct admin
    clients. ``shared`` marks a conversation with more than one participant.
    """
    session_id: str | None = None
    shared: bool = False


@dataclass(frozen=True)
class ChatCommandRequest:
    name: str
    session: SessionRef = SessionRef()
    argument: str | None = None
    confirmed: bool = False


@dataclass(frozen=True)
class StopOutcome:
    stopped: bool = False
    cleared: int = 0


@dataclass(frozen=True)
class SessionStatus:
    busy: bool = False
    queued: int = 0
    sessions: int = 0


@dataclass(frozen=True)
class ChatCommandResult:
    text: str
    status: str = COMPLETED
    # Structured additions for rich clients. Text-only channels render ``text``.
    picker: dict[str, Any] | None = None
    context: dict[str, Any] | None = None
    compacted: bool = False
    # Filled only by a deferred host: work it must still apply, in this order,
    # before the result may be shown. See :class:`DeferredChatCommandHost`.
    host_actions: tuple[str, ...] = ()


class ChatCommandHost:
    """The minimal port a host implements. Every method is optional.

    A method left unimplemented makes the commands needing it report
    ``unsupported`` truthfully, instead of acknowledging a no-op.
    """

    retains_history = False
    # Exception text can carry private provider diagnostics. A host that shows
    # results to end users on a third-party channel turns this off.
    expose_errors = True

    async def stop_session(self, session: SessionRef) -> StopOutcome:
        """Stop the running turn and drop queued input for ``session``."""
        raise ChatCommandUnavailable

    async def rotate_session(self, session: SessionRef) -> str | None:
        """Make the next turn start in a fresh session; return its id if known."""
        raise ChatCommandUnavailable

    async def session_status(self, session: SessionRef) -> SessionStatus:
        raise ChatCommandUnavailable

    async def authorize_model_change(self, session: SessionRef) -> None:
        """Raise ``PermissionError`` if the caller may not change the model."""

    async def forget_history(self, session: SessionRef, agent: Any) -> int:
        """Forget provider and model state; return the sessions forgotten."""
        await agent.forget_session(session.session_id)
        return 1


class DeferredChatCommandHost(ChatCommandHost):
    """A host whose session model lives across a process boundary.

    Core cannot call such a host during the command. It receives a snapshot of
    the conversation and returns ``host_actions`` for the host to apply. A host
    must apply all of them, in order, and show the result only if they all
    succeed. The host learns no command names this way: new shared commands
    need no host change.

    ``can_apply=False`` is for a path with nobody to apply the actions, such as
    a run whose text happens to start with a slash. Commands needing a host
    action then report ``unsupported`` rather than pretending.
    """

    retains_history = True
    expose_errors = False

    def __init__(self, *, active_runs: int = 0, queued_runs: int = 0, sessions: int = 1,
                 can_apply: bool = True) -> None:
        self.active_runs = active_runs
        self.queued_runs = queued_runs
        self.sessions = sessions
        self.can_apply = can_apply
        self.actions: list[str] = []

    def _defer(self, action: str) -> None:
        if not self.can_apply:
            raise ChatCommandUnavailable
        self.actions.append(action)

    async def stop_session(self, session: SessionRef) -> StopOutcome:
        self._defer(STOP_RUNS)
        return StopOutcome(stopped=self.active_runs > 0, cleared=self.queued_runs)

    async def rotate_session(self, session: SessionRef) -> str | None:
        self._defer(ROTATE_SESSION)
        return None

    async def session_status(self, session: SessionRef) -> SessionStatus:
        if not self.can_apply:
            raise ChatCommandUnavailable
        return SessionStatus(busy=self.active_runs > 0, queued=self.queued_runs, sessions=self.sessions)


def _join_clauses(clauses: list[str]) -> str:
    return ". ".join(c.capitalize() if i == 0 else c for i, c in enumerate(clauses)) + "."


def _cleared_clause(cleared: int) -> str:
    return f"cleared {cleared} queued message{'s' if cleared != 1 else ''}"


class ChatCommandService:
    """Executes one authorized command. Callers authorize before calling this."""

    def __init__(self, agent: Any, host: ChatCommandHost, *, scopes: Iterable[str] | None = None) -> None:
        self.agent = agent
        self.host = host
        self.scopes = None if scopes is None else frozenset(scopes)

    async def execute(self, request: ChatCommandRequest, *,
                      on_compaction_status: Callable[[str], Awaitable[None]] | None = None) -> ChatCommandResult:
        name = request.name
        spec = COMMAND_MAP.get(name)
        if spec is None:
            return ChatCommandResult(f"Unknown command: {name}", UNSUPPORTED)
        handler = {
            "new": self._reset, "reset": self._reset, "clear": self._reset, "stop": self._stop,
            "status": self._status, "queue": self._queue, "help": self._help, "context": self._context,
            "model": self._model,
        }.get(name)
        if name == "compact":
            handler = lambda r: self._compact(r, on_compaction_status)  # noqa: E731
        if handler is None or (self.scopes is not None and spec.scope not in self.scopes):
            return self._unsupported(name)
        try:
            return await handler(request)
        except ChatCommandUnavailable:
            return self._unsupported(name)
        except PermissionError:
            return ChatCommandResult(f"/{name} is not allowed for this user.", FAILED)
        except Exception as exc:  # noqa: BLE001 - reported to the caller as an outcome
            return self._failed(name, exc)

    def _unsupported(self, name: str) -> ChatCommandResult:
        tail = " The conversation was not reset." if name in RESET_COMMANDS else ""
        return ChatCommandResult(f"/{name} is not available here.{tail}", UNSUPPORTED)

    def _failed(self, name: str, exc: Exception) -> ChatCommandResult:
        tail = " The conversation was not reset." if name in RESET_COMMANDS else ""
        detail = f" {type(exc).__name__}: {exc}" if self.host.expose_errors else ""
        return ChatCommandResult(f"/{name} failed.{tail}{detail}", FAILED)

    def _deferred(self) -> tuple[str, ...]:
        return tuple(getattr(self.host, "actions", ()))

    async def _reset(self, request: ChatCommandRequest) -> ChatCommandResult:
        session = request.session
        if session.shared:
            # Checked first: a refused reset must not stop another member's turn.
            raise ChatCommandUnavailable
        outcome = await self.host.stop_session(session)
        clauses = []
        if outcome.stopped:
            clauses.append("stopped current operation")
        if outcome.cleared:
            clauses.append(_cleared_clause(outcome.cleared))
        if self.host.retains_history:
            await self.agent.release_session(session.session_id)
            await self.host.rotate_session(session)
            clauses.append("started a fresh conversation")
            text = (_join_clauses(clauses) + " The previous conversation is kept but no longer used here. "
                    "Long-term memory and the vault are unchanged.")
            return ChatCommandResult(text, host_actions=self._deferred())
        forgotten = await self.host.forget_history(session, self.agent)
        fresh = await self.host.rotate_session(session)
        if forgotten:
            clauses.append(f"forgot {forgotten} prior conversation{'s' if forgotten != 1 else ''}")
        clauses.append(f"fresh session: {(fresh or '')[-8:]}")
        return ChatCommandResult(_join_clauses(clauses), host_actions=self._deferred())

    async def _stop(self, request: ChatCommandRequest) -> ChatCommandResult:
        outcome = await self.host.stop_session(request.session)
        clauses = []
        if outcome.stopped:
            clauses.append("Stopped current operation")
        if outcome.cleared:
            clauses.append(_cleared_clause(outcome.cleared))
        text = ". ".join(clauses) + "." if clauses else "Nothing running."
        return ChatCommandResult(text, host_actions=self._deferred())

    async def _status(self, request: ChatCommandRequest) -> ChatCommandResult:
        status = await self.host.session_status(request.session)
        return ChatCommandResult(f"{'Busy' if status.busy else 'Idle'} | Queue: {status.queued} | Sessions: {status.sessions}")

    async def _queue(self, request: ChatCommandRequest) -> ChatCommandResult:
        status = await self.host.session_status(request.session)
        return ChatCommandResult(f"Queue depth: {status.queued}")

    async def _help(self, request: ChatCommandRequest) -> ChatCommandResult:
        return ChatCommandResult(command_help_text(self.scopes))

    async def _context(self, request: ChatCommandRequest) -> ChatCommandResult:
        session_id = request.session.session_id
        if not session_id:
            return ChatCommandResult("No active session to inspect.")
        try:
            from .core.context_report import build_context_report, format_context_report_text

            report = build_context_report(self.agent, session_id)
            if report is None:
                return ChatCommandResult("No context to report for this session yet.")
            return ChatCommandResult(format_context_report_text(report), context=report)
        except Exception as exc:  # noqa: BLE001
            return self._detail("Could not read context", exc)

    def _detail(self, prefix: str, exc: Exception) -> ChatCommandResult:
        text = f"{prefix}: {exc}" if self.host.expose_errors else f"{prefix}."
        return ChatCommandResult(text, FAILED)

    async def _compact(self, request: ChatCommandRequest, on_status: Any) -> ChatCommandResult:
        session_id = request.session.session_id
        if not session_id:
            return ChatCommandResult("No active session to compact.")
        from .session_control import SessionControl

        try:
            result = await SessionControl(self.agent).compact(session_id, on_status=on_status)
        except Exception as exc:  # noqa: BLE001
            return self._detail("Compaction failed", exc)
        if result is None:
            return ChatCommandResult("Nothing to compact — the conversation is empty or already compacted.")
        # Concise outcome only: channels want the start and end of the fold, and
        # rich clients draw the detail from their own compaction frames.
        return ChatCommandResult("Compacted conversation.", compacted=True)

    async def _model(self, request: ChatCommandRequest) -> ChatCommandResult:
        from .session_control import SessionControl

        session_id, arg = request.session.session_id, request.argument
        control = SessionControl(self.agent)
        if not session_id:
            return ChatCommandResult("No active session — cannot read or change model pin.")
        db = getattr(self.agent, "memory_db", None)
        if db is None:
            return ChatCommandResult("Memory DB not available.")
        if arg is None or arg.strip() == "":
            return await self._list_models(control, session_id)
        await self.host.authorize_model_change(request.session)
        if arg.strip().lower() in ("default", "none", "reset", "auto"):
            try:
                await control.clear_model_pin(session_id)
            except Exception as exc:  # noqa: BLE001
                return self._detail("Failed to clear model pin", exc)
            return ChatCommandResult("Model pin cleared — back to Auto (best model picked automatically).")
        return await self._pin_model(control, db, session_id, arg.strip())

    async def _list_models(self, control: Any, session_id: str) -> ChatCommandResult:
        # ``available_models`` is the enriched query: raw ``models`` rows carry
        # neither ``runtime_id`` nor ``provider_name``.
        try:
            models = await control.available_models()
            current_pin = await control.model_pin(session_id)
        except Exception as exc:  # noqa: BLE001
            return self._detail("Failed to list models", exc)
        if not models:
            return ChatCommandResult("No models configured.")
        lines = ["Configured models (enabled):"]
        options = []
        for model in models:
            runtime_id = model.get("runtime_id") or ""
            if not runtime_id:
                continue
            display = model.get("display_name") or model.get("model") or runtime_id
            active = runtime_id == current_pin
            lines.append(f"  • {runtime_id}  ({display}){' ← active' if active else ''}")
            options.append({"label": display, "value": runtime_id,
                            "subtitle": model.get("provider_name") or runtime_id, "active": active})
        if not current_pin:
            lines.append("Current: Auto (no pin)")
        lines.append("Usage: /model <runtime_id> | /model default")
        # Structured picker for bridges that can render a keyboard or select.
        # The explicit "Auto" option clears the pin (maps to /model default).
        picker = {"command": "model", "prompt": "Pick a model for this conversation:",
                  "options": [{"label": "Auto", "value": "default", "active": not current_pin}, *options]}
        return ChatCommandResult("\n".join(lines), picker=picker)

    async def _pin_model(self, control: Any, db: Any, session_id: str, runtime_id: str) -> ChatCommandResult:
        try:
            row = await db.get_model_by_runtime_id(runtime_id)
            if row is None:
                models = await db.list_models(enabled_only=True)
                valid = [m.get("runtime_id") or "" for m in models if m.get("runtime_id")]
                return ChatCommandResult(f"Unknown model {runtime_id!r}. Valid options: {', '.join(valid) or '(none configured)'}")
            if not row.get("enabled"):
                return ChatCommandResult(f"Model {runtime_id!r} is disabled — enable it first.")
            await control.pin_model(session_id, runtime_id)
            display = row.get("display_name") or row.get("model") or runtime_id
            return ChatCommandResult(f"Switched to {display} ({runtime_id}) for this session.")
        except ValueError as exc:
            return self._detail("Cannot pin model", exc)
        except Exception as exc:  # noqa: BLE001
            return self._detail("Failed to switch model", exc)
