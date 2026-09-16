"""Framework-level prompts injected into every OpenAgent conversation.

These are prepended to the user-supplied ``system_prompt`` from
``openagent.yaml``. They codify the operating guidelines that apply to
every OpenAgent deployment regardless of project context: how to use the
memory vault, when to prefer MCP tools over shell, how autonomously to
act, etc. The user's config is expected to stay short and
project-specific (identity, key facts, pointers to memory).
"""

from openagent_core.configuration import runtime_environment
import os

from openagent_core.prompts import default_framework_text

FRAMEWORK_SYSTEM_PROMPT = default_framework_text()
# Compatibility name only: actual agent execution never selects a reduced prompt.
LEAN_LOCAL_EVENT_SYSTEM_PROMPT = FRAMEWORK_SYSTEM_PROMPT


# Builtin, high-traffic MCPs whose EXACT tool keys we inline into the
# catalog so the model copies a key verbatim instead of guessing (and
# mis-prefixing) it — vision §"deferred tools" sanctions surfacing a
# handful of high-traffic builtin tools up front. Third-party MCPs and
# the browser MCP are omitted (unbounded / very large tool counts); the
# model discovers those on demand via ``tool_search_list_tools``.
_INLINE_TOOL_KEYS_SERVERS = frozenset({
    "vault", "vault-gate", "shell", "scheduler", "editor",
    "workflow-manager", "mcp-manager", "model-manager", "agent-manager", "ui-manager", "delegation",
    "web-search", "attachments", "messaging", "memory-search",
    "agent-federation", "media-gen", "computer-control", "env",
})
# Per-server cap so a large MCP can't bloat the every-turn prompt; beyond
# it the model falls back to ``list_tools``.
_INLINE_TOOL_KEYS_CAP = 24


def _operator_inline_servers() -> frozenset[str]:
    """Server names the OPERATOR opted into full tool-key inlining, read from
    the ``OPENAGENT_INLINE_TOOL_KEYS_SERVERS`` env var (comma-separated).

    ``_INLINE_TOOL_KEYS_SERVERS`` above is OpenAgent's built-in allowlist —
    small, high-traffic builtins that ship with the framework. A deployment
    that connects its OWN high-traffic MCP (an org/domain MCP whose exact tool
    keys the model must copy verbatim rather than guess) lists it here from its
    own config, so OpenAgent never hardcodes a tenant's server name. These are
    operator-vetted, so the renderer inlines ALL their names with no per-server
    cap — a left-out key is exactly what the model would otherwise hallucinate.
    """
    raw = runtime_environment().get("OPENAGENT_INLINE_TOOL_KEYS_SERVERS", "")
    return frozenset(s.strip() for s in raw.split(",") if s.strip())


def _render_catalog_summary_lines(
    summary: dict[str, int],
    descriptions: dict[str, str],
    tool_names: dict[str, list[str]] | None = None,
) -> str:
    """Render the markdown bullet list for the MCP catalog summary.

    Shared by :func:`build_mcp_catalog_summary` and
    :meth:`openagent_core.mcp.pool.MCPPool._build_catalog_summary` so the cached
    pool output and the duck-typed test helper can't drift apart.

    Order: ``vault`` first (the model's only durable memory),
    ``tool-search`` last (the model is already using it to read this
    text), everything else alphabetical in between.
    """
    if not summary:
        return "(no MCPs connected)"

    names = sorted(summary.keys())
    ordered: list[str] = []
    if "vault" in names:
        ordered.append("vault")
        names.remove("vault")
    if "tool-search" in names:
        names.remove("tool-search")  # save for last
    ordered.extend(n for n in names if n != "tool-search")
    if "tool-search" in summary:
        ordered.append("tool-search")

    # Hardcoded one-liners for the two DEFAULT_MCPS entries (vault,
    # filesystem) that don't live in BUILTIN_MCP_SPECS, so their
    # description never reaches ``server_descriptions``. Foregrounded so
    # the model sees them on every turn without burning a list_servers
    # round-trip.
    _NPX_DEFAULTS = {
        "filesystem": (
            "read and write files on the host filesystem within the "
            "configured roots. Use for ad-hoc reads when the editor "
            "MCP would be overkill"
        ),
    }

    operator_servers = _operator_inline_servers()
    lines: list[str] = []
    for name in ordered:
        count = summary[name]
        if name == "vault":
            lines.append(
                f"- ``vault`` ({count} tools): YOUR LONG-TERM MEMORY. "
                f"READ BEFORE acting on anything that touches prior work, "
                f"user preferences, or ongoing projects. WRITE AFTER any "
                f"non-obvious learning."
            )
        elif name == "tool-search":
            lines.append(
                f"- ``tool-search`` ({count} tools): the deferred-tool "
                f"discovery MCP itself. Use `list_servers` / `list_tools` "
                f"/ `describe_tool` / `call_tool` to reach any other MCP."
            )
        else:
            desc = descriptions.get(name, "") or _NPX_DEFAULTS.get(name, "")
            if desc:
                lines.append(f"- ``{name}`` ({count} tools): {desc}.")
            else:
                lines.append(f"- ``{name}`` ({count} tools).")

        # Inline the exact registered keys so the model copies one verbatim
        # instead of guessing (and mis-prefixing) it. Two sources: OpenAgent's
        # built-in high-traffic allowlist (capped at _INLINE_TOOL_KEYS_CAP), and
        # any server the operator opted in via env — org/domain MCPs, inlined in
        # FULL (no cap) since a left-out key is what the model would hallucinate.
        # ``tool-search`` is skipped — its four keys are already spelled out in
        # the framework prompt's tool section above.
        if tool_names and (name in _INLINE_TOOL_KEYS_SERVERS or name in operator_servers):
            keys = tool_names.get(name) or []
            if keys:
                if name in operator_servers:
                    shown, more = keys, 0  # operator-vetted → surface every key
                else:
                    shown = keys[:_INLINE_TOOL_KEYS_CAP]
                    more = len(keys) - len(shown)
                suffix = f", … (+{more} more — use list_tools)" if more > 0 else ""
                lines.append(f"    tools: {', '.join(shown)}{suffix}")

    return "\n".join(lines)


def build_mcp_catalog_summary(pool) -> str:
    """Render the live MCP catalog for injection into the framework prompt.

    Called per-turn by Agent._combined_system_prompt to substitute
    ``{{MCP_CATALOG_SUMMARY}}``. Defensive — must not raise even when
    the pool is None, broken (server_summary raises), or empty.

    Prefers ``pool.render_catalog_summary()`` (cached on the pool,
    invalidated on hot-reload) so the per-turn cost is one attribute
    read on the steady-state path. Falls back to the duck-typed
    rebuild for test pools that don't expose the cached helper.
    """
    if pool is None:
        return "(no MCPs connected)"

    cached_renderer = getattr(pool, "render_catalog_summary", None)
    if callable(cached_renderer):
        try:
            return cached_renderer()
        except Exception:
            pass

    try:
        summary = pool.server_summary() or {}
    except Exception:
        return "(MCP catalog unavailable)"

    if not summary:
        return "(no MCPs connected)"

    descriptions: dict[str, str] = {}
    try:
        descriptions = pool.server_descriptions() or {}
    except Exception:
        descriptions = {}

    tool_names: dict[str, list[str]] = {}
    try:
        getter = getattr(pool, "server_tool_names", None)
        if callable(getter):
            tool_names = getter() or {}
    except Exception:
        tool_names = {}

    return _render_catalog_summary_lines(summary, descriptions, tool_names)


def build_skills_index(registry) -> str:
    """Render the ``## Skills`` section for the ``{{SKILLS_INDEX}}`` slot.

    Mirrors :func:`build_mcp_catalog_summary`: called per-turn by
    ``Agent._combined_system_prompt`` to substitute the placeholder.
    Progressive disclosure — the model sees only a category → ``name:
    description`` index here and loads full bodies on demand via
    ``skill_view`` (reached through ``tool_search_call_tool``).

    CACHE DISCIPLINE — critical. This lands in the CACHED system prefix
    (above ``<session-id>``), so it must be byte-identical across every
    session/turn on a box. The registry's ``render_skills_index`` is a
    frozen snapshot (cached, invalidated only on load/reload, no volatile
    tokens), and this wrapper adds only static prose.

    Returns "" when ``registry`` is None — i.e. skills disabled. Because
    the placeholder sits flush against the next header
    (``{{SKILLS_INDEX}}## Builtin management MCPs``), an empty render leaves
    the framework prompt BYTE-IDENTICAL to a build without this feature.
    Defensive: never raises (a broken registry degrades to "").
    """
    if registry is None:
        return ""
    try:
        index = registry.render_skills_index()
    except Exception:
        return ""

    return (
        "## Skills\n\n"
        "You have a library of SKILLS — SKILL.md playbooks for recurring "
        "tasks. Only the INDEX below is loaded up front (progressive "
        "disclosure): each entry is ``name``: a one-line description, grouped "
        "by category. When a task matches one, load its full body ON DEMAND "
        "with ``skill_view`` (reached via "
        "``tool_search_call_tool(tool_ref=<discovered skill_view reference>, "
        "args={\"name\": \"...\"})``) BEFORE acting, then follow it. Use "
        "``skill_search`` to find a skill you can't see, and ``skill_manage`` "
        "to create/update/remove one. Do not guess a skill's contents from "
        "its description — open it.\n\n"
        f"{index}\n\n"
    )


def build_ptc_note(enabled: bool) -> str:
    """Render the ``## Programmatic tool calling`` section for the
    ``{{PTC_NOTE}}`` slot — "" when PTC is disabled.

    CACHE DISCIPLINE — like :func:`build_skills_index`, this lands in the CACHED
    system prefix (above ``<session-id>``), so it is a STATIC render: no
    per-turn tokens. Returns "" when ``enabled`` is False, and because the
    placeholder sits flush against the next header
    (``{{SKILLS_INDEX}}{{PTC_NOTE}}## Builtin management MCPs``), that empty
    render leaves the framework prompt BYTE-IDENTICAL to a build without this
    feature.
    """
    if not enabled:
        return ""
    return (
        "## Programmatic tool calling\n\n"
        "You have ``run_python(code)`` — write a Python script that reaches "
        "your OWN tools via ``call_tool(tool_ref, args)`` (already in "
        "scope, no import needed; the exact opaque reference discovered through "
        "``tool_search_call_tool``). The script runs in a sandbox and ONLY its "
        "stdout is returned to you. Reach for it to collapse a multi-step tool "
        "pipeline into ONE turn — fan out over many items, filter/aggregate in "
        "code, or join results from several tools — instead of paying a model "
        "round-trip per tool call. Print just the distilled answer.\n\n"
    )
