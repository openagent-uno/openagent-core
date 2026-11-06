"""Deprecated compatibility for a retired process-termination heuristic.

Process names and command-line substrings cannot establish ownership. Core
never terminates another application's processes during host startup. Hosts
own service lifecycle and must acquire their own exclusive installation lock.
These old entry points remain harmless for already-published host versions.
"""
from __future__ import annotations
from pathlib import Path
import warnings


def find_stale_serve_pids(agent_dir: Path, *, self_pid: int | None = None) -> list[int]:
    """Return no candidates: process scanning cannot prove stale ownership."""
    warnings.warn("Process discovery by command-line matching was retired; the host owns instance lifecycle.",
                  DeprecationWarning, stacklevel=2)
    return []


def kill_stale_serve_processes(agent_dir: Path, *, grace_seconds: float = 5.0) -> list[int]:
    """Compatibility no-op. Never signal processes inferred from their names."""
    warnings.warn("Automatic process termination was retired; acquire an exclusive host installation lock instead.",
                  DeprecationWarning, stacklevel=2)
    return []
