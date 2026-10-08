from __future__ import annotations

from pathlib import Path
import tomllib

import openagent_core


def test_runtime_version_matches_distribution_metadata() -> None:
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    assert openagent_core.__version__ == project["project"]["version"]
