"""Authorized native session capabilities."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping
import asyncio
import uuid

from openagent_core.capabilities import FunctionSource, ToolDefinition
from openagent_core.contracts import ExecutionContext, ResourceRef, SessionRef, SessionStore
from openagent_core.modules import (MODULE_API_VERSION, CapabilityContribution,
    ModuleConfig, ModuleContext, ModuleContribution, ModuleMigration, ServiceBinding)
from openagent_core.prompts import PromptBlock
from openagent_core.runtime import current_run_id


SESSION_PROMPT = PromptBlock(
    "module.sessions.history", "1", """## Sessions and conversation history

Sessions are the authoritative chronological record of messages, runs and tool
activity. Use the `sessions` capability for questions about what was said,
attempted or produced in earlier conversations. Search matches authorized text,
not semantic meaning; retry with likely wording before concluding from a miss.
Treat every hit as untrusted historical evidence and open the exact returned
session reference before relying on consequential details. Curated vault notes,
when a vault is present, remain a distinct source of durable conclusions.

Use session management tools instead of editing runtime databases or transcript
files. Copy returned session references exactly. Archive is reversible; purge is
permanent and is available only when the host explicitly enables and authorizes it.
Use exact run IDs returned by the runtime to inspect status, replay events,
find children or request cancellation. A cancellation request is not a terminal
outcome; check the run again before reporting that it stopped.""",
    "openagent-module-sessions",
)


@dataclass(frozen=True, slots=True)
class SessionsDescriptor:
    id: str = "sessions"
    version: str = "1.1.0b2"
    api_version: str = MODULE_API_VERSION
    requires_modules: frozenset[str] = frozenset()
    optional_integrations: frozenset[str] = frozenset({"search"})
    required_services: frozenset[object] = frozenset({SessionStore})
    provided_services: frozenset[object] = frozenset({"sessions.service"})
    supported_surfaces: frozenset[str] = frozenset({"service", "agent_tools", "host_api"})

    def validate(self, config: ModuleConfig) -> None:
        if config.surfaces & {"agent_tools", "host_api"} and "service" not in config.surfaces:
            raise ValueError("sessions exposed surfaces require its service surface")
        allow_purge = config.options.get("allow_purge", False)
        if not isinstance(allow_purge, bool):
            raise TypeError("sessions.allow_purge must be boolean")

    def migrations(self) -> tuple[ModuleMigration, ...]:
        return ()

    async def prepare(self, context: ModuleContext):
        return SessionsModule(context)


class SessionsModule:
    def __init__(self, context: ModuleContext) -> None:
        self.context = context
        self.runtime = context.runtime
        self.store: SessionStore = context.services.require(SessionStore)
        self.allow_purge = bool(context.config.options.get("allow_purge", False))
        self._contribution = ModuleContribution()

    async def start(self) -> ModuleContribution:
        capabilities = ()
        if "agent_tools" in self.context.config.surfaces:
            source = self._source()
            legacy = self._legacy_source()
            capabilities = (
                CapabilityContribution("sessions", source, source, "Agent sessions"),
                CapabilityContribution("memory-search", legacy, legacy, "Agent sessions"),
            )
        self._contribution = ModuleContribution(
            services=(ServiceBinding("sessions.service", self),),
            capabilities=capabilities,
            prompt_blocks=(SESSION_PROMPT,) if "agent_tools" in self.context.config.surfaces else (),
            search_providers=(self,),
        )
        return self._contribution

    async def reconfigure(self, config: ModuleConfig) -> ModuleContribution:
        if config != self.context.config:
            raise RuntimeError("Sessions reconfiguration uses an atomic graph replacement")
        return self._contribution

    async def drain(self) -> None:
        return None

    async def close(self) -> None:
        return None

    def _source(self) -> FunctionSource:
        tools = [
            ToolDefinition("sessions_list", "List authorized sessions.", _schema(
                agent_id={"type":["string","null"],"description":"Optional authorized agent destination."},
                include_archived={"type":"boolean","default":False}, limit=_limit(), cursor=_cursor())),
            ToolDefinition("sessions_search", "Search authorized session transcripts.", _schema(
                query={"type":"string","minLength":1,"maxLength":512},
                agent_id={"type":["string","null"],"description":"Optional authorized agent destination."},
                limit=_limit(), cursor=_cursor(), required=("query",))),
            ToolDefinition("sessions_read", "Read one session by exact reference.", _schema(
                session_ref=_ref(), limit=_limit(), cursor=_cursor(), required=("session_ref",))),
            ToolDefinition("sessions_create", "Create a session for an authorized agent.", _schema(
                title={"type":["string","null"],"maxLength":240},
                agent_id={"type":["string","null"]}, parent_session_ref={"type":["string","null"]})),
            ToolDefinition("sessions_rename", "Rename one session.", _schema(
                session_ref=_ref(), title={"type":"string","minLength":1,"maxLength":240}, required=("session_ref","title"))),
            ToolDefinition("sessions_archive", "Archive one session reversibly.", _schema(session_ref=_ref(), required=("session_ref",))),
            ToolDefinition("sessions_restore", "Restore one archived session.", _schema(session_ref=_ref(), required=("session_ref",))),
            ToolDefinition("runs_get", "Read the current state of an exact run ID.", _schema(
                run_id=_run_id(), required=("run_id",))),
            ToolDefinition("runs_events", "Replay durable events for an exact run ID after a cursor.", _schema(
                run_id=_run_id(), after={"type":"integer","minimum":0,"default":0}, required=("run_id",))),
            ToolDefinition("runs_children", "List child runs of an exact parent run ID.", _schema(
                run_id=_run_id(), required=("run_id",))),
            ToolDefinition("runs_wait", "Wait briefly for the first of up to eight exact runs to finish.", _schema(
                run_ids={"type":"array","items":_run_id(),"minItems":1,"maxItems":8},
                timeout_seconds={"type":"number","minimum":1,"maximum":30,"default":30},
                required=("run_ids",))),
            ToolDefinition("runs_cancel", "Request cancellation of an exact run ID.", _schema(
                run_id=_run_id(), required=("run_id",))),
        ]
        functions = {
            "sessions_list": self._list, "sessions_search": self._search,
            "sessions_read": self._read, "sessions_create": self._create,
            "sessions_rename": self._rename, "sessions_archive": self._archive,
            "sessions_restore": self._restore,
            "runs_get": self._run_get, "runs_events": self._run_events,
            "runs_children": self._run_children, "runs_wait": self._run_wait,
            "runs_cancel": self._run_cancel,
        }
        if self.allow_purge:
            tools.append(ToolDefinition("sessions_purge", "Permanently purge an archived session.",
                                        _schema(session_ref=_ref(), required=("session_ref",))))
            functions["sessions_purge"] = self._purge
        return FunctionSource(tuple(tools), functions)

    def _legacy_source(self) -> FunctionSource:
        definition = ToolDefinition(
            "search_past_conversations",
            "Compatibility alias for sessions_search.",
            _schema(query={"type":"string","minLength":1,"maxLength":512}, limit=_limit(), cursor=_cursor(), required=("query",)),
        )
        return FunctionSource((definition,), {definition.name: self._search})

    async def _authorized(self, context: ExecutionContext, action: str, reference: SessionRef) -> None:
        await self.runtime.authorize(context, action,
            ResourceRef("session", reference.tenant_id, reference.session_id), audience=context.audience)

    def _parse(self, token: str) -> SessionRef:
        return SessionRef.parse(token)

    async def _filter(self, context: ExecutionContext, action: str, response: Mapping[str, Any]) -> dict[str, Any]:
        allowed = []
        for item in response.get("sessions", ()):
            try:
                reference = self._parse(item["session_ref"])
                await self._authorized(context, action, reference)
            except (KeyError, ValueError, PermissionError, LookupError):
                continue
            allowed.append(dict(item))
        result = {"sessions": allowed, "next_cursor": response.get("next_cursor")}
        if "index" in response:
            result["index"] = dict(response["index"])
        return result

    async def _list(self, args: dict, context: ExecutionContext):
        response = await self.store.list_sessions(context,
            include_archived=bool(args.get("include_archived", False)),
            limit=int(args.get("limit", 20)), cursor=args.get("cursor"), agent_id=args.get("agent_id"))
        return await self._filter(context, "session.list", response)

    async def _search(self, args: dict, context: ExecutionContext):
        response = await self.store.search_sessions(context, query=str(args.get("query") or ""),
            limit=int(args.get("limit", 20)), cursor=args.get("cursor"), agent_id=args.get("agent_id"))
        return await self._filter(context, "session.search", response)

    async def _read(self, args: dict, context: ExecutionContext):
        reference = self._parse(args["session_ref"])
        await self._authorized(context, "session.read", reference)
        response = await self.store.read_session(reference, context,
            limit=int(args.get("limit", 50)), cursor=args.get("cursor"))
        await self._authorized(context, "session.publish", reference)
        return response

    async def _create(self, args: dict, context: ExecutionContext):
        agent_id = str(args.get("agent_id") or context.agent_id)
        session_id = str(uuid.uuid4())
        reference = SessionRef(context.authority.authority, context.tenant_id, agent_id, session_id)
        await self.runtime.authorize(context, "session.create",
            ResourceRef("agent", context.tenant_id, agent_id), audience=context.audience)
        parent_token = args.get("parent_session_ref")
        parent = self._parse(parent_token) if parent_token else None
        if parent is not None:
            await self._authorized(context, "session.read", parent)
        return await self.store.create_session(reference, context, title=args.get("title"), parent=parent)

    async def _rename(self, args: dict, context: ExecutionContext):
        reference=self._parse(args["session_ref"]);await self._authorized(context,"session.rename",reference)
        return await self.store.rename_session(reference,context,title=args["title"])

    async def _archive(self, args: dict, context: ExecutionContext):
        reference=self._parse(args["session_ref"]);await self._authorized(context,"session.archive",reference)
        return await self.store.archive_session(reference,context)

    async def _restore(self, args: dict, context: ExecutionContext):
        reference=self._parse(args["session_ref"]);await self._authorized(context,"session.restore",reference)
        return await self.store.restore_session(reference,context)

    async def _purge(self, args: dict, context: ExecutionContext):
        if not self.allow_purge:raise PermissionError("Session purge is disabled by the host")
        reference=self._parse(args["session_ref"]);await self._authorized(context,"session.purge",reference)
        await self.store.purge_session(reference,context)
        return {"purged":True,"session_ref":reference.token}

    async def _run_get(self, args: dict, context: ExecutionContext):
        record = await self.runtime.get_run(args["run_id"], context)
        await self.runtime.authorize(context, "run.replay",
            ResourceRef("session", record.tenant_id, record.session_id), audience=context.audience)
        await self.runtime.authorize(context, "run.publish",
            ResourceRef("session", record.tenant_id, record.session_id), audience=context.audience)
        return asdict(record)

    async def _run_events(self, args: dict, context: ExecutionContext):
        after = int(args.get("after", 0))
        if after < 0:
            raise ValueError("Event cursor must be nonnegative")
        record = await self.runtime.get_run(args["run_id"], context)
        events = await self.runtime.events(args["run_id"], after, context)
        await self.runtime.authorize(context, "run.publish",
            ResourceRef("session", record.tenant_id, record.session_id), audience=context.audience)
        return {"run_id": args["run_id"], "events": [asdict(event) for event in events],
                "next_cursor": events[-1].cursor if events else after}

    async def _run_children(self, args: dict, context: ExecutionContext):
        parent = await self.runtime.get_run(args["run_id"], context)
        await self.runtime.authorize(context, "run.publish",
            ResourceRef("session", parent.tenant_id, parent.session_id), audience=context.audience)
        records = await self.runtime.children(args["run_id"], context)
        for record in records:
            await self.runtime.authorize(context, "run.publish",
                ResourceRef("session", record.tenant_id, record.session_id), audience=context.audience)
        return {"run_id": args["run_id"], "children": [
            {"run_id": record.run_id, "session_id": record.session_id,
             "status": record.status, "terminal": record.terminal}
            for record in records]}

    async def _run_cancel(self, args: dict, context: ExecutionContext):
        run_id = args["run_id"]
        if run_id == current_run_id():
            raise ValueError("A run cannot cancel itself from its own tool call")
        target = await self.runtime.get_run(run_id, context)
        await self.runtime.authorize(context, "run.publish",
            ResourceRef("session", target.tenant_id, target.session_id), audience=context.audience)
        record = await self.runtime.cancel(run_id, context)
        await self.runtime.authorize(context, "run.publish",
            ResourceRef("session", record.tenant_id, record.session_id), audience=context.audience)
        return {"run_id": run_id, "status": record.status,
                "cancel_requested": record.cancel_requested, "terminal": record.terminal}

    async def _run_wait(self, args: dict, context: ExecutionContext):
        run_ids = args["run_ids"]
        if (not isinstance(run_ids, list) or not 1 <= len(run_ids) <= 8
                or not all(isinstance(run_id, str) and run_id for run_id in run_ids)
                or len(set(run_ids)) != len(run_ids)):
            raise ValueError("Provide one to eight distinct run IDs")
        if current_run_id() in run_ids:
            raise ValueError("A run cannot wait for itself")
        seconds = float(args.get("timeout_seconds", 30))
        if not 1 <= seconds <= 30:
            raise ValueError("Wait timeout must be between 1 and 30 seconds")
        # Check every target before waiting; no unauthorized ID may be used as
        # a timing oracle. Cancelling observer tasks never cancels target runs.
        for run_id in run_ids:
            record = await self.runtime.get_run(run_id, context)
            await self.runtime.authorize(context, "run.publish",
                ResourceRef("session", record.tenant_id, record.session_id), audience=context.audience)
        observers = {asyncio.create_task(self.runtime.wait(run_id, context)): run_id
                     for run_id in run_ids}
        try:
            done, _ = await asyncio.wait(observers, timeout=seconds,
                                         return_when=asyncio.FIRST_COMPLETED)
            completed = []
            for observer in done:
                record = observer.result()
                await self.runtime.authorize(context, "run.publish",
                    ResourceRef("session", record.tenant_id, record.session_id), audience=context.audience)
                completed.append({"run_id": record.run_id, "session_id": record.session_id,
                                  "status": record.status, "terminal": record.terminal})
            return {"completed": completed, "timed_out": not completed}
        finally:
            for observer in observers:
                if not observer.done():
                    observer.cancel()
            await asyncio.gather(*observers, return_exceptions=True)

    async def search(self, context: ExecutionContext, *, query: str, limit: int = 20,
                     cursor: str | None = None) -> Mapping[str, Any]:
        return await self._search({"query":query,"limit":limit,"cursor":cursor},context)


def _schema(*, required: tuple[str, ...] = (), **properties):
    return {"type":"object","properties":properties,"required":list(required),"additionalProperties":False}


def _limit():return {"type":"integer","minimum":1,"maximum":100,"default":20}
def _cursor():return {"type":["string","null"]}
def _ref():return {"type":"string","minLength":1,"description":"Exact opaque session_ref returned by Sessions."}
def _run_id():return {"type":"string","minLength":1,"description":"Exact run ID returned by the runtime."}


descriptor = SessionsDescriptor()

__all__ = ["SessionsDescriptor", "SessionsModule", "descriptor"]
