"""Cross-platform path resolution for OpenAgent config, data, and logs.

Follows platform conventions (XDG on Linux, Application Support on macOS,
%APPDATA% on Windows). Every function returns a :class:`Path` and ensures
the directory exists.

When an **agent directory** is set (via :func:`set_agent_dir`), all paths
are resolved relative to that directory instead of platform defaults. This
enables running multiple independent agents in parallel, each with its own
config, database, memories, and logs.

Precedence for config loading (handled by :func:`config.load_config`):

1. Explicit ``--config`` / ``-c`` CLI flag — highest priority.
2. ``<agent_dir>/openagent.yaml`` — if agent dir is set.
3. ``openagent.yaml`` in the current working directory.
4. ``<config_dir>/openagent.yaml`` — XDG/system default.

For data (DB, vault), the default is ``<data_dir>/`` unless overridden in
the YAML config via ``memory.db_path`` / ``memory.vault_path``.
"""

from __future__ import annotations

from openagent_core.configuration import runtime_environment
import os
from contextvars import ContextVar
import platform
import textwrap
from pathlib import Path

APP_NAME = "openagent"

# ── Agent directory singleton ──
# When set, all path functions return paths relative to this directory
# instead of platform-standard locations.

_agent_dir_context: ContextVar[Path | None] = ContextVar("openagent_agent_directory", default=None)


def set_agent_dir(path: Path | None) -> None:
    """Set the active agent directory. Pass ``None`` to reset to defaults."""
    _agent_dir_context.set(path.resolve() if path is not None else None)


def get_agent_dir() -> Path | None:
    """Return the active agent directory, or ``None`` if using defaults."""
    from openagent_core.runtime import current_runtime
    runtime = current_runtime()
    return runtime.settings.workspace if runtime is not None else _agent_dir_context.get()


# ── Platform path helpers ──

def _system() -> str:
    return platform.system()  # "Darwin", "Linux", "Windows"


def _platform_dir(kind: str) -> Path:
    """Resolve the base config/data directory for the current platform."""
    agent_dir = get_agent_dir()
    if agent_dir is not None:
        agent_dir.mkdir(parents=True, exist_ok=True)
        return agent_dir

    system = _system()
    if system == "Darwin":
        base = Path.home() / "Library" / "Application Support" / "OpenAgent"
    elif system == "Windows":
        base = Path(runtime_environment().get("APPDATA", Path.home() / "AppData" / "Roaming")) / "OpenAgent"
    else:
        env_name = "XDG_CONFIG_HOME" if kind == "config" else "XDG_DATA_HOME"
        default = Path.home() / ".config" if kind == "config" else Path.home() / ".local" / "share"
        xdg = runtime_environment().get(env_name, str(default))
        base = Path(xdg) / APP_NAME
    base.mkdir(parents=True, exist_ok=True)
    return base


def config_dir() -> Path:
    """Return the config directory, creating it if needed."""
    return _platform_dir("config")


def data_dir() -> Path:
    """Return the data directory, creating it if needed."""
    return _platform_dir("data")


def log_dir() -> Path:
    """Return the log directory (inside data_dir)."""
    d = data_dir() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def default_config_path() -> Path:
    """Return the default config file path inside the config directory."""
    return config_dir() / "openagent.yaml"


def default_db_path() -> Path:
    """Return the default SQLite database path."""
    return data_dir() / "openagent.db"


def default_vault_path() -> Path:
    """Return the default memory vault directory."""
    d = data_dir() / "memories"
    d.mkdir(parents=True, exist_ok=True)
    return d


def default_skills_path() -> Path:
    """Return the default skills directory.

    Mirrors :func:`default_vault_path` (``<data_dir>/skills`` — honours
    ``--agent-dir`` via the ``_agent_dir`` global), with one addition:
    an explicit ``OPENAGENT_SKILLS_PATH`` env override wins. The override
    is the seam that keeps the in-process skills MCP handlers (which have
    no config) pointed at the same directory the Agent resolved from
    ``skills.path`` in YAML — the Agent exports it into the process env at
    startup (parity with how ``OPENAGENT_VAULT_PATH`` reaches the vault
    subprocess).
    """
    override = runtime_environment().get("OPENAGENT_SKILLS_PATH", "").strip()
    d = Path(override).expanduser() if override else data_dir() / "skills"
    d.mkdir(parents=True, exist_ok=True)
    return d
