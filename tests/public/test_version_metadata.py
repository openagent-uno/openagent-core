from __future__ import annotations

from pathlib import Path
import tomllib

import openagent_core


def test_runtime_version_matches_distribution_metadata() -> None:
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))

    assert openagent_core.__version__ == project["project"]["version"]


def test_coupled_distributions_pin_the_same_runtime_version() -> None:
    root = Path(__file__).resolve().parents[2]
    for package in ("packages/storage-sqlite", "packages/gateway", "sdk/python"):
        project = tomllib.loads((root / package / "pyproject.toml").read_text(encoding="utf-8"))["project"]
        assert project["version"] == openagent_core.__version__, package
        assert f"openagent-core=={openagent_core.__version__}" in project["dependencies"], package


def test_full_module_and_reference_profiles_pin_the_same_sessions_release() -> None:
    root = Path(__file__).resolve().parents[2]
    def project(path):
        return tomllib.loads((root / path / "pyproject.toml").read_text())["project"]
    sessions = project("packages/module-sessions")
    full = project("packages/module-full")
    compatibility = project("packages/modules")
    reference = project("examples/reference-host")
    requirement = f"openagent-module-sessions=={sessions['version']}"
    assert requirement in full["dependencies"]
    assert requirement in reference["dependencies"]
    assert f"openagent-modules-full=={full['version']}" in compatibility["dependencies"]
    assert f"openagent-modules=={compatibility['version']}" in project(".")["optional-dependencies"]["modules"]
