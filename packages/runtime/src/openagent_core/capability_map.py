"""What the model can do this turn, in one readable map.

Tools are deferred behind discovery, so without a map the model sees one tool
(``tool-search``) and has to guess which source owns a job. This module turns
the turn's *authorized* catalog into a short plain-text ``<capabilities>``
section: every source with a use-case description, its target (which computer
or account it acts on), its tool count and, for small sources, the tool names.
It also owns the source descriptions and the alias table discovery uses, so a
renamed source ("scheduler" → "schedules") still resolves.

Nothing here grants anything: the map is rendered from ``catalog.discover``
for the current execution context, so it never names a tool the model cannot
call in this turn.
"""
from __future__ import annotations

import re
from typing import Any, Iterable, Mapping, Optional

# Use-case descriptions for the framework's own sources, keyed by the source id
# discovery reports. A host may override any of them (DiscoveryPresentation
# ``source_summaries``); third-party MCP servers describe themselves through
# their own tool descriptions.
SOURCE_SUMMARIES: dict[str, str] = {
    "vault": "long-term memory notes: search, read, write and patch durable facts, decisions and procedures",
    "vault-gate": "check and repair the memory vault's quality (links, orphans, duplicates, frontmatter)",
    "sessions": "conversation history: search and read earlier sessions, runs and their events",
    "attachments": "send files to the user in this conversation, or find and resend files already exchanged",
    "schedules": "cron-scheduled and one-shot tasks: create, list, update, run now and inspect their runs",
    "workflows": "multi-step automated workflows: define, run and inspect them",
    "events": "inbound webhook triggers: create endpoints that start the agent when an external service calls",
    "models": "LLM models and providers: list, add, enable, and pin a model to a conversation",
    "budget": "spending limits and usage: read and set budgets",
    "search": "search across authorized history and memory in one query",
    "skills": "your skills (SKILL.md playbooks): view, search, create and update them",
    "skill-data": "files and data that belong to a skill",
    "logs": "your own event log: recent errors, summaries and the context around an event",
    "messaging": "send messages to the user's connected channels",
    "delegation": "delegate a self-contained task to a sub-agent and get its result",
    "mcp": "MCP servers: list, add, configure and remove external tool servers",
    "mcp-manager": "MCP servers: list, add, configure and remove external tool servers",
    "shell": "run shell commands on the target computer; long jobs can run in the background",
    "filesystem": "read, write, move and search files and directories on the target computer",
    "editor": "precise code editing on the target computer: view, edit, apply patches, glob and grep",
    "computer-control": "see and operate the target computer's screen: screenshots, mouse and keyboard",
    "agent-in-chrome": "drive a dedicated Chromium browser on the target computer",
    "media-gen": "generate images, audio or video",
    "web": "search the web and fetch pages",
    "tool-search": "find and call every other tool (discovery)",
}

# Older or alternative names, mapped to the source id discovery uses now. Both
# the prompt and the model's habits still use some of these.
SOURCE_ALIASES: dict[str, str] = {
    "scheduler": "schedules",
    "schedule": "schedules",
    "scheduled-tasks": "schedules",
    "cron": "schedules",
    "workflow-manager": "workflows",
    "workflow": "workflows",
    "events-manager": "events",
    "webhooks": "events",
    "webhook": "events",
    "model-manager": "models",
    "budget-manager": "budget",
    "mcp-manager": "mcp",
    "memory-search": "sessions",
    "history": "sessions",
    "memory": "vault",
    "notes": "vault",
    "files": "filesystem",
    "terminal": "shell",
    "bash": "shell",
    "browser": "agent-in-chrome",
    "skill": "skills",
}

# Sources small enough to list their tool names inline.
_INLINE_TOOL_LIMIT = 8
_DESCRIPTION_CHARS = 120
_DEFAULT_MAP_CHARS = 9000


def canonical_source(name: Optional[str]) -> Optional[str]:
    """The current source id for ``name`` (itself when it is not an alias)."""
    if name is None:
        return None
    key = str(name).strip()
    return SOURCE_ALIASES.get(key.casefold(), key)


def source_summary(source_id: str, overrides: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """The use-case description of a source: host override, then framework default."""
    if overrides and source_id in overrides and overrides[source_id]:
        return str(overrides[source_id])
    summary = SOURCE_SUMMARIES.get(source_id)
    if summary:
        return summary
    try:
        from openagent_core.mcp.builtins import BUILTIN_MCP_SPECS
    except Exception:  # noqa: BLE001 - descriptions are a convenience
        return None
    spec = BUILTIN_MCP_SPECS.get(source_id) or {}
    description = spec.get("description")
    return " ".join(str(description).split()) if description else None


def hide_aliases(source_ids: Iterable[str]) -> set[str]:
    """Source ids that only duplicate a present canonical source under an older name.

    Modules may register a source twice for compatibility (``scheduler`` and
    ``schedules`` are the same tools). Both stay callable; presentation shows
    only the canonical one so the model does not see two sources for one job.
    """
    present = set(source_ids)
    return {name for name in present if canonical_source(name) != name and canonical_source(name) in present}


def alias_terms(source_id: str) -> list[str]:
    """The aliases that point at ``source_id`` (used to make search forgiving)."""
    return [alias for alias, target in SOURCE_ALIASES.items() if target == source_id]


def _clip(text: str, limit: int) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def group_sources(tools: Iterable[Any], overrides: Optional[Mapping[str, str]] = None) -> list[dict[str, Any]]:
    """Group discovered tool descriptors by source, listing every target it acts on."""
    groups: dict[str, dict[str, Any]] = {}
    for tool in tools:
        source_id = str(getattr(tool, "source_id", "") or "")
        target = str(getattr(tool, "target_label", "") or "")
        if not source_id:
            continue
        entry = groups.setdefault(source_id, {
            "source_ref": source_id, "targets": [], "tools": set(),
            "description": source_summary(source_id, overrides),
        })
        if target and target not in entry["targets"]:
            entry["targets"].append(target)
        entry["tools"].add(str(getattr(tool, "name", "")))
        if not entry["description"]:
            metadata = getattr(tool, "metadata", None) or {}
            declared = metadata.get("openagent/source-instructions") if hasattr(metadata, "get") else None
            if isinstance(declared, str) and declared.strip():
                first = re.split(r"(?<=[.!?])\s", " ".join(declared.split()), maxsplit=1)[0]
                entry["description"] = f"(as the server describes itself) {first}"
    for hidden in hide_aliases(groups):
        groups.pop(hidden, None)
    out = []
    for entry in groups.values():
        entry["tools"] = sorted(filter(None, entry["tools"]), key=str.casefold)
        entry["tool_count"] = len(entry["tools"])
        entry["target_label"] = ", ".join(entry["targets"])
        out.append(entry)
    # Framework sources first (the agent's own memory, history and automation),
    # then everything else, alphabetically; discovery itself last.
    return sorted(out, key=lambda e: (e["source_ref"] == "tool-search",
                                      e["source_ref"] not in SOURCE_SUMMARIES, e["source_ref"]))


def render(groups: list[dict[str, Any]], *, max_chars: int = _DEFAULT_MAP_CHARS,
           named_calls: bool = False, ready: Iterable[Any] = ()) -> str:
    """The plain-text ``<capabilities>`` section for the system prompt."""
    groups = [g for g in groups if g["source_ref"] != "tool-search"]
    if not groups:
        return ""
    call = (
        "call it with `tool_search_call_named_tool(source_ref, tool_name, args)` using the source and "
        "tool name shown here or by list_tools, plus `target_label` for a source on several targets "
        "(names stay valid across turns; `tool_search_call_tool(tool_ref, args)` also works)"
        if named_calls else "call it with `tool_search_call_tool(tool_ref, args)`"
    )
    head = (
        "<capabilities>\n"
        "These are the capability sources you can use in this turn. Their tools are not in your "
        "function list: find one with `tool_search_list_tools(source_ref=..., query=...)` (or "
        "`tool_search_list_tools(query=...)` across all sources), check its schema with "
        "`tool_search_describe_tool(tool_ref)` when unsure, and " + call + ". After @ is the computer "
        "or account each source acts on; a source on several targets has a separate tool per target, "
        "so pick the target asked for.\n"
    )
    lines = []
    for group in groups:
        target = f" @ {group['target_label']}" if group["target_label"] else ""
        description = f": {_clip(group['description'], _DESCRIPTION_CHARS)}" if group["description"] else ""
        line = f"- {group['source_ref']}{target} ({group['tool_count']} tools){description}"
        if group["tool_count"] <= _INLINE_TOOL_LIMIT:
            line += f". Tools: {', '.join(group['tools'])}"
        lines.append(line)
    tail = "</capabilities>"
    ready_lines = _ready_lines(ready) if named_calls else []
    if ready_lines:
        ready_lines.insert(0, "Ready to call by name, no search needed (source/tool(arguments, * required)):")
    ready_size = sum(len(line) + 1 for line in ready_lines)
    budget = max_chars - len(head) - len(tail) - 620 - ready_size  # room for "Also available"
    kept, used = [], 0
    for line in lines:
        if used + len(line) + 1 > budget:
            break
        kept.append(line)
        used += len(line) + 1
    if len(kept) < len(lines):
        omitted = ", ".join(g["source_ref"] for g in groups[len(kept):])
        kept.append(_clip(f"- Also available (call `tool_search_list_servers` for details): {omitted}", 600))
    kept.extend(ready_lines)
    return head + "\n".join(kept) + "\n" + tail


_READY_LIMIT = 12
_READY_LINE_CHARS = 260


def _ready_lines(tools: Iterable[Any]) -> list[str]:
    """One line per host-chosen hot tool the turn may call: its exact name and arguments."""
    lines = []
    for tool in list(tools)[:_READY_LIMIT]:
        schema = getattr(tool, "input_schema", None) or {}
        properties = schema.get("properties") if isinstance(schema, Mapping) else None
        required = set(schema.get("required") or ()) if isinstance(schema, Mapping) else set()
        names = [f"{name}*" if name in required else str(name) for name in (properties or {})]
        target = f" @ {tool.target_label}" if getattr(tool, "target_label", None) else ""
        first = re.split(r"(?<=[.!?])\s", " ".join(str(getattr(tool, "description", "") or "").split()), maxsplit=1)[0]
        line = f"- {tool.source_id}/{tool.name}({', '.join(names)}){target}"
        if first:
            line += f": {first}"
        lines.append(_clip(line, _READY_LINE_CHARS))
    return lines


async def render_for_turn(runtime: Any, context: Any, *, overrides: Optional[Mapping[str, str]] = None,
                          max_chars: int = _DEFAULT_MAP_CHARS, named_calls: bool = False,
                          hot_tools: Iterable[tuple[str, str]] = ()) -> str:
    """The capability map for the current execution, or ``""`` when unavailable."""
    if runtime is None or context is None:
        return ""
    catalog = getattr(runtime, "capabilities", None)
    if catalog is None:
        return ""
    try:
        tools = await catalog.discover(context)
    except Exception:  # noqa: BLE001 - the map is advisory; discovery stays authoritative
        return ""
    wanted = list(hot_tools)
    ready = [tool for pair in wanted for tool in tools
             if (getattr(tool, "source_id", None), getattr(tool, "name", None)) == pair]
    return render(group_sources(tools, overrides), max_chars=max_chars, named_calls=named_calls, ready=ready)


_STEM_SUFFIXES = ("ations", "ation", "ings", "ing", "ies", "ers", "er", "es", "ed", "s")


STOPWORDS = frozenset({"a", "an", "the", "to", "for", "of", "and", "or", "in", "on", "with", "my",
                       "me", "i", "it", "this", "that", "from", "by", "at", "is", "be", "do"})


def stem(term: str) -> str:
    """A light, deterministic stem so 'schedule' matches 'scheduled'/'schedules'."""
    term = term.casefold()
    for suffix in _STEM_SUFFIXES:
        if len(term) > len(suffix) + 2 and term.endswith(suffix):
            term = term[: -len(suffix)] + ("y" if suffix == "ies" else "")
            break
    if len(term) > 4 and term.endswith("e"):
        term = term[:-1]
    return term


def terms(text: str) -> list[str]:
    """Lowercased word terms, splitting camelCase, snake_case and kebab-case."""
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(text or ""))
    return re.findall(r"[^\W_]+", text.casefold())
