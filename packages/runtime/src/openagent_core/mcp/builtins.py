"""Built-in MCP specs and resolution helpers."""

from __future__ import annotations

from openagent_core.configuration import runtime_environment
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import platform

from openagent_core._frozen import bundle_dir, is_frozen

logger = logging.getLogger(__name__)

if is_frozen():
    # Frozen layout: PyInstaller extracts the source tree at
    # ``<bundle>/src/...`` (the spec adds each ``src/mcp/servers/<name>``
    # entry with that destination prefix), so the bundled MCP directory
    # must be looked up at ``<bundle>/src/mcp/servers``. This used to read
    # ``openagent/mcp/servers`` from before the openagent→src package
    # rename (commit 4b5efb5); the stale literal silently broke every
    # Python/Node built-in MCP — workflow-manager, messaging, scheduler,
    # model-manager, mcp-manager, web-search, media-gen, memory-search —
    # because ``resolve_builtin_entry`` raised FileNotFoundError on the
    # missing directory. In-process MCPs (shell, tool-search) bypass the
    # directory check, which is why they kept working and masked the bug.
    BUILTIN_MCPS_DIR = bundle_dir() / "src" / "mcp" / "servers"
    PACKAGE_PARENT_DIR = bundle_dir()
else:
    BUILTIN_MCPS_DIR = Path(__file__).resolve().parent / "servers"
    # Dev layout: this file is src/mcp/builtins.py, so .parent.parent.parent
    # is the directory containing the `src/` package (the repo root).
    PACKAGE_PARENT_DIR = Path(__file__).resolve().parent.parent.parent

# CRITICAL: ``PACKAGE_PARENT_DIR`` is exported as PYTHONPATH for Python MCP
# subprocesses so they can ``import openagent_core.mcp.servers.*``. It MUST be the
# directory that *contains* ``src/`` — never ``src/`` itself, since that
# would expose ``openagent_core.mcp`` as a top-level ``mcp`` and shadow the
# third-party MCP SDK, causing a circular import in src/mcp/client.py.


BUILTIN_MCP_SPECS: dict[str, dict[str, Any]] = {
    "tool-search": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.tool_search.adapters",
        "runtime_toolkit_factory": "build_runtime_toolkit",
    },
    "vault-gate": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.vault_gate.adapters",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "evaluate and repair your memory vault — run the quality gate "
            "(orphans, broken links, over-long notes, duplicates, missing "
            "frontmatter), mechanically fix what code can, validate a note "
            "before writing it, search, and regenerate llms.txt / showcase"
        ),
    },
    "attachments": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.attachments.adapters",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "read and write files attached to the current turn — "
            "screenshots, images, pasted text, uploads from the chat UI"
        ),
    },
    # Native Skills subsystem — Hermes / Claude-Code SKILL.md progressive
    # disclosure. In-process (the skills dir resolves from paths.default_
    # skills_path, which honours the live agent dir set by set_agent_dir —
    # a subprocess would re-resolve it from platform defaults, the same
    # bug that forced OPENAGENT_DB_PATH injection elsewhere).
    #
    # DELIBERATELY NOT in DEFAULT_MCPS: registration is gated on
    # ``skills.enabled`` (see ``config_gated_mcp_entries`` + bootstrap).
    # With skills disabled this spec is inert — a dict entry that nothing
    # ever seeds a row for — so the running system is byte-identical.
    "skills": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.skills.adapters",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "your file-backed skills — SKILL.md playbooks surfaced by an "
            "index in the system prompt. skill_view loads a full skill body "
            "on demand, skill_search finds one by name/description/body, and "
            "skill_manage creates/updates/removes them on disk"
        ),
    },
    # Programmatic Tool Calling — the ``run_python`` tool. In-process because
    # it starts a Unix-socket RPC server on the running gateway loop and
    # dispatches through the live ``MCPPool`` (a subprocess could reach neither).
    #
    # DELIBERATELY NOT in DEFAULT_MCPS: registration is gated on ``ptc.enabled``
    # (see ``config_gated_mcp_entries`` + bootstrap), mirroring ``skills``. With
    # PTC disabled this spec is inert — a dict entry nothing ever seeds a row
    # for — so the running system is byte-identical.
    "ptc": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.ptc.adapters",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "run_python(code) — write a Python script that reaches your own "
            "tools via call_tool(tool_ref, args); the script runs in a "
            "sandbox and only its stdout returns to you. Collapses a multi-step "
            "tool pipeline into one turn"
        ),
    },
    # In-process on purpose: the log path comes from ``paths.log_dir()``,
    # which resolves against the live agent dir set by ``set_agent_dir``. A
    # subprocess would re-resolve it from platform defaults and read a
    # DIFFERENT agent's log — the bug that forced OPENAGENT_DB_PATH injection
    # for the scheduler / model-manager subprocess MCPs (resolve_default_entry).
    "logs": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.logs.adapters",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "query your own unified event log — search past events by "
            "name, time window, session, or error; summarise what went "
            "wrong and what it cost; read the events surrounding a "
            "failure. Reach for it to diagnose your own behaviour "
            "instead of tailing events.jsonl through the shell"
        ),
    },
    "messaging": {
        "dir": "messaging",
        "command": ["node", "dist/index.js"],
        "build": ["npm", "run", "build"],
        "install": ["npm", "install"],
        "description": (
            "send messages on connected platforms (Telegram, Discord, "
            "Slack, WhatsApp) when the user asks you to relay something"
        ),
    },
    # The long-term memory vault. A vendored fork of @bitbonsai/mcpvault
    # (src/mcp/servers/vault, see VENDORED.md) with an OpenAgent addition:
    # every write is run through the vault quality gate (validate.ts) — it
    # auto-fixes the mechanical issues (frontmatter scaffolding, dates,
    # wikilink spacing, em dashes) and rejects structurally broken notes so
    # the agent literally cannot save a messy one. The vault path arrives via
    # OPENAGENT_VAULT_PATH (injected in resolve_default_entry).
    "vault": {
        "dir": "vault",
        "command": ["node", "dist/server.js"],
        "build": ["npm", "run", "build"],
        "install": ["npm", "install"],
        "env": {"OPENAGENT_VAULT_VALIDATE_WRITES": "1"},
        "description": (
            "the long-term memory vault — read, write, patch, search, "
            "move, and tag your markdown notes. Every write is validated "
            "and auto-corrected to the vault's quality standard"
        ),
    },
    "scheduler": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.scheduler.server",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "create, list, update, and remove cron-scheduled prompts. "
            "Reach for it whenever the user asks for a recurring task"
        ),
    },
    "mcp-manager": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.mcp_manager.server",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "inspect and manage MCP servers — list connected ones, add "
            "new ones, enable/disable, check health"
        ),
    },
    "model-manager": {
        "dir": "model_manager",
        "command": ["python", "-m", "openagent_core.mcp.servers.model_manager.server"],
        "python": True,
        "description": (
            "manage the registered LLM models — list, enable/disable, "
            "pin one for the current session, set the entry/router model"
        ),
    },

    "workflow-manager": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.workflow_manager.server",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "create and run multi-step workflows. Use for repeatable "
            "structured processes that benefit from explicit DAGs over "
            "ad-hoc sub-agent delegation"
        ),
    },
    "events-manager": {
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.events_manager.server",
        "runtime_toolkit_factory": "build_runtime_toolkit",
        "description": (
            "create, list, update, and remove webhook events, and fire one on "
            "demand. An event is an inbound trigger (a name, a webhook type, an "
            "input schema, a per-event secret) bound to an action — run a "
            "workflow, a scheduled task, or a chat prompt — when an external "
            "service (or a peer) calls it"
        ),
    },
    "budget-manager": {
        "dir": "budget_manager",
        "command": ["python", "-m", "openagent_core.mcp.servers.budget_manager.server"],
        "python": True,
        "description": (
            "inspect and adjust your own spend caps — list budgets, read "
            "current spend vs limit (before doing expensive work), and "
            "create/update/remove a per-model, per-provider, or global "
            "dollar/token cap over an hour/day/month window. A tripped cap "
            "routes you AROUND that model, it never stops you"
        ),
    },
    "media-gen": {
        "dir": "media_gen",
        "command": ["python", "-m", "openagent_core.mcp.servers.media_gen.server"],
        "python": True,
        # Was "images, audio, or video" — but the server only ever registered
        # generate_image (OpenAI) and generate_video (Fal); there is no audio
        # tool. tool-search surfaces this text as the MCP's one-line pitch, so
        # the phantom capability was an invitation for the model to burn a
        # turn calling a tool that does not exist. Speech synthesis is not
        # missing from OpenAgent — it lives in the TTS path, not in an MCP.
        "description": (
            "generate images or video via configured providers"
        ),
    },
    "memory-search": {
        # In-process is a security boundary, not an optimization. The tool
        # reads the authenticated on-behalf-of ContextVar and calls the same
        # redacted operational search layer as the gateway. A subprocess
        # would need a reusable principal-bearing token and could not safely
        # recheck canonical ACLs for the current turn.
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.memory_search.adapters",
        "description": (
            "authorized full-text search across chats, tools, workflows, "
            "scheduled runs and events. Complements the separate Markdown "
            "vault; matches redacted words, not meaning"
        ),
    },
    "delegation": {
        "dir": "delegation",
        "in_process": True,
        "adapter_module": "openagent_core.mcp.servers.delegation.adapters",
        "description": (
            "hand a sub-task to another registered model and get its "
            "answer back. Use when a different model is cheaper, "
            "faster, or better-scoped for the work"
        ),
    },

    "skill-data": {
        "dir": "skill_data",
        "command": ["python", "-m", "openagent_core.mcp.servers.skill_data.server"],
        "python": True,
    },
}

DEFAULT_MCPS: list[dict[str, Any]] = [
    {"builtin": "vault", "_default": True},
    {"builtin": "tool-search", "_default": True},
    {"builtin": "vault-gate", "_default": True},
    {"builtin": "attachments", "_default": True},
    # On by default: §14 makes reading the log a first-class agent capability,
    # and every other introspection surface (scheduler, workflow-manager,
    # events-manager, mcp-manager, model-manager) is already here. It is
    # ~free to ship — in-process (no subprocess, no Node, no DB), and since
    # the v0.14 defer-all rewrite only tool-search is in the upfront tool
    # list, so three more tools cost 0 prompt tokens until actually used.
    # Dream mode's log-triage mission also depends on it being present.
    {"builtin": "logs", "_default": True},
    # On by default. It now calls the redacted operational FTS5 service in
    # process, so it can use the authenticated turn principal and canonical
    # ACL recheck without a reusable bearer credential. The old TranscriptIndex
    # remains only as a shadow/compatibility bridge for direct legacy tests.
    #
    # It has to be default-on because ``prompts.py`` tells the model this tool
    # exists and when to reach for it. A described tool that isn't registered
    # is the same defect as a prompt naming a tool that doesn't exist — the
    # model burns a turn on "Function not found" and learns nothing.
    #
    # In-process registration is effectively free until tool discovery/use.
    {"builtin": "memory-search", "_default": True},
    {"builtin": "messaging", "_default": True},
    {"builtin": "scheduler", "_default": True},
    {"builtin": "mcp-manager", "_default": True},
    {"builtin": "model-manager", "_default": True},
    {"builtin": "workflow-manager", "_default": True},
    {"builtin": "events-manager", "_default": True},
    # On by default: a spend cap the agent can't see is one it can't reason
    # about. In-line with §15 (the agent knows its own levers) and free until
    # used — one more Python subprocess of the same kind as the others, and
    # tool schemas are deferred so it costs 0 prompt tokens until the agent
    # queries its budget.
    {"builtin": "budget-manager", "_default": True},
    {"builtin": "skill-data", "_default": True},
    {"builtin": "delegation", "_default": True},
    # On by default now that ``generate_image`` resolves its backend by
    # CAPABILITY rather than by vendor key. It was correctly left out while it
    # meant "call the metered OpenAI API or fail": seeding a tool nobody had a
    # key for only taught the model that images do not work here.
    #
    # It now uses whichever enabled model declares ``image_generation`` in its
    # metadata — a subscription-backed proxy included — and when nothing
    # declares it the tool says exactly that instead of failing blank. So the
    # cost of seeding it is one Python subprocess of the same kind as the six
    # already here, and the benefit is that an agent whose catalog CAN draw
    # actually can. Operators who don't want it disable the row.
    {"builtin": "media-gen", "_default": True},
]


def config_gated_mcp_entries(config: dict | None) -> list[dict[str, Any]]:
    """Extra builtin MCP entries that are OFF by default and only registered
    when their opt-in config stanza is set.

    Kept OUT of ``DEFAULT_MCPS`` on purpose: those are seeded unconditionally
    on every boot, whereas these must leave the system byte-identical unless
    the operator opts in. ``ensure_builtin_mcps`` appends whatever this
    returns to the seed set, so an unset flag seeds nothing new.

    Today this is the native Skills subsystem (``skills.enabled``) and
    Programmatic Tool Calling (``ptc.enabled``).
    """
    from openagent_core.core.config import ptc_settings, skills_settings

    entries: list[dict[str, Any]] = []
    if skills_settings(config).enabled:
        entries.append({"builtin": "skills", "_default": True})
    if ptc_settings(config).enabled:
        entries.append({"builtin": "ptc", "_default": True})
    return entries


def command_exists(cmd: str) -> bool:
    return shutil.which(cmd) is not None


def _find_node_binary() -> str | None:
    """Return absolute path to a working node binary, or None."""
    node_name = "node.exe" if platform.system() == "Windows" else "node"
    candidates = [
        runtime_environment().get("OPENAGENT_NODE_BINARY", ""),
        str(Path(sys.executable).resolve().parent / node_name),
        str(bundle_dir() / node_name),
        "/opt/homebrew/bin/node",
        "/usr/local/bin/node",
        "/usr/bin/node",
        "/snap/bin/node",
    ]
    if SYSTEM := platform.system() == "Windows":
        candidates += [
            os.path.expandvars(r"%ProgramFiles%\nodejs\node.exe"),
            os.path.expandvars(r"%ProgramFiles(x86)%\nodejs\node.exe"),
            os.path.expandvars(r"%APPDATA%\npm\node.exe"),
        ]
    for p in candidates:
        if p and os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    # Search nvm
    nvm_dir = Path.home() / ".nvm" / "versions" / "node"
    if nvm_dir.is_dir():
        for v in sorted(nvm_dir.iterdir(), reverse=True):
            p = v / "bin" / "node"
            if p.is_file() and os.access(p, os.X_OK):
                return str(p)
    # Fall back to whatever is on PATH
    return shutil.which("node")


def resolve_builtin_entry(name: str, env: dict[str, str] | None = None) -> dict[str, Any]:
    """Resolve a built-in MCP by name into MCPTools kwargs."""
    if name not in BUILTIN_MCP_SPECS:
        available = ", ".join(BUILTIN_MCP_SPECS.keys())
        raise ValueError(f"Unknown built-in MCP: {name}. Available: {available}")

    spec = BUILTIN_MCP_SPECS[name]

    # In-process specs don't need a directory, a subprocess, or Node — return
    # early with a lightweight descriptor that MCPPool knows how to consume.
    if spec.get("in_process"):
        resolved = {
            "name": name,
            "_trusted_module": name,
            "in_process": True,
            "adapter_module": spec["adapter_module"],
            "runtime_toolkit_factory": spec.get("runtime_toolkit_factory", "build_runtime_toolkit"),
        }
        # In-process adapters do not inherit a manufactured subprocess
        # environment, but some of them (notably the shared filesystem core)
        # intentionally accept explicit operator configuration through env.
        # Keep that configuration without adding an empty ``env`` field so
        # principal-bound adapters such as memory-search stay ambient-free.
        if env:
            resolved["env"] = dict(env)
        return resolved

    is_python = spec.get("python", False)
    if name == "vault":
        configured_assets = (env or {}).get("OPENAGENT_MODULE_ASSETS_VAULT")
        if configured_assets:
            mcp_dir = Path(configured_assets).expanduser().resolve()
        else:
            # One beta compatibility window for old standalone bundles. New
            # module wheels always pass their own resource directory explicitly.
            try:
                from openagent_modules import module_assets
            except ImportError as exc:
                raise RuntimeError(
                    "The vault module must provide OPENAGENT_MODULE_ASSETS_VAULT"
                ) from exc
            mcp_dir = module_assets("vault")
    elif is_python and is_frozen():
        # Python modules are importable from the frozen PYZ archive. They do
        # not require a duplicate source directory in the extraction tree.
        # The product executable exposes the exact internal MCP entrypoint;
        # DB, credentials and workspace remain explicitly supplied in env.
        from importlib.util import find_spec
        module_name = f"openagent_core.mcp.servers.{spec['dir']}.server"
        if find_spec(module_name) is None:
            raise FileNotFoundError(f"Built-in MCP '{name}' module is missing from the frozen distribution")
        mcp_dir = bundle_dir()
    else:
        mcp_dir = BUILTIN_MCPS_DIR / spec["dir"]
    if not mcp_dir.exists():
        raise FileNotFoundError(f"Built-in MCP '{name}' directory not found at {mcp_dir}")

    if not is_python:
        # Dependency installation and compilation belong to product packaging.
        # Resolving a runtime module never downloads or edits its installed code.
        if not (mcp_dir / "dist").exists():
            raise FileNotFoundError(
                f"Built-in MCP '{name}' needs prebuilt Node resources; provision them in the host package"
            )

    cmd_list = list(spec["command"])
    if name == "vault":
        cmd_list = ["node", "dist/server.mjs"]
    if is_python and cmd_list and cmd_list[0] in ("python3", "python"):
        exe_basename = os.path.basename(sys.executable).lower()
        if is_frozen() or "python" not in exe_basename:
            cmd_list = [sys.executable, "_mcp-server", name]
        else:
            cmd_list[0] = sys.executable

    # Resolve bare "node" / "npx" to absolute paths so subprocesses
    # launched from a venv (where node may not be on PATH) still work.
    if cmd_list and cmd_list[0] in ("node", "npx"):
        found = _find_node_binary()
        if found:
            cmd_list[0] = found

    full_command: list[str] = []
    for part in cmd_list:
        if "/" in part and not Path(part).is_absolute():
            full_command.append(str(mcp_dir / part))
        else:
            full_command.append(part)

    merged_env = {**(spec.get("env") or {}), **(env or {})}
    if is_python:
        package_parent = str(PACKAGE_PARENT_DIR)
        existing_pp = merged_env.get("PYTHONPATH") or runtime_environment().get("PYTHONPATH", "")
        merged_env["PYTHONPATH"] = package_parent + (os.pathsep + existing_pp if existing_pp else "")

    return {
        "name": name,
        "_trusted_module": name,
        "command": full_command,
        "env": merged_env if merged_env else None,
        "_cwd": str(mcp_dir),
    }


def resolve_default_entry(entry: dict[str, Any], db_path: str | None = None) -> dict[str, Any] | None:
    """Resolve a default MCP entry. Returns MCPTools kwargs or None if skipped."""
    name = entry.get("name") or entry.get("builtin", "")

    if "builtin" in entry:
        spec = BUILTIN_MCP_SPECS.get(entry["builtin"])
        is_python = spec.get("python", False) if spec else False
        is_native = spec.get("native", False) if spec else False
        is_in_process = spec.get("in_process", False) if spec else False
        if not is_python and not is_native and not is_in_process and not _find_node_binary():
            logger.warning("Skipping default MCP '%s': Node.js not found", name)
            return None

        # In-process MCPs receive live dependencies (including the canonical
        # DB object and authenticated turn context) from MCPPool. Do not
        # manufacture subprocess-only values such as OPENAGENT_DB_PATH for
        # them. Explicit adapter configuration is still preserved — e.g. an
        # existing filesystem row may carry OPENAGENT_FILESYSTEM_ROOTS.
        if is_in_process:
            try:
                return resolve_builtin_entry(
                    entry["builtin"], env=entry.get("env") or None,
                )
            except Exception as exc:
                logger.warning("Skipping default MCP '%s': %s", name, exc)
                return None

        extra_env: dict[str, str] = dict(entry.get("env") or {})
        # Every builtin gets OPENAGENT_DB_PATH, whether or not it looks like
        # it needs one. This used to be a hand-kept list of the servers known
        # to touch the shared SQLite DB — and the very next builtin that
        # started touching it (media-gen, which resolves the image backend by
        # reading the model catalogue) was not on it. It fell back to
        # ``./openagent.db`` relative to the subprocess CWD, which under
        # PyInstaller is a scratch directory: it CREATED an empty database
        # there, found no models, and reported "no image backend configured"
        # for a fleet that had one. A list you must remember to update is a
        # list that will be wrong; the variable costs nothing to a server that
        # ignores it. events-manager also resolves its events.key next to it.
        if db_path:
            extra_env.setdefault("OPENAGENT_DB_PATH", os.path.abspath(db_path))
        else:
            from openagent_core.core.paths import default_db_path

            extra_env.setdefault("OPENAGENT_DB_PATH", str(default_db_path()))

        # The vault MCP needs to know which folder is the vault. It reads
        # OPENAGENT_VAULT_PATH (server.ts), so the subprocess lands on the
        # same notes directory as the rest of OpenAgent instead of its CWD.
        # Operational memory-search is deliberately separate and in-process;
        # it never opens the Markdown vault or its index.
        if entry["builtin"] == "vault" and "OPENAGENT_VAULT_PATH" not in extra_env:
            from openagent_core.core.paths import default_vault_path

            extra_env["OPENAGENT_VAULT_PATH"] = str(default_vault_path())

        try:
            return resolve_builtin_entry(entry["builtin"], env=extra_env or None)
        except Exception as exc:
            logger.warning("Skipping default MCP '%s': %s", name, exc)
            return None

    from openagent_core.core.paths import default_vault_path

    args = entry.get("args") or []
    if name == "vault" and not args:
        args = [str(default_vault_path())]

    cmd = entry.get("command", [None])[0]
    if cmd and not command_exists(cmd):
        logger.warning("Skipping default MCP '%s': '%s' not found", name, cmd)
        return None

    return {
        "name": entry.get("name", ""),
        "command": entry.get("command"),
        "args": args,
        "url": entry.get("url"),
        "env": entry.get("env"),
    }
