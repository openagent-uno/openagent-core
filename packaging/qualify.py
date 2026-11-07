#!/usr/bin/env python3
"""Qualify one immutable Core wheel cohort in a new, independent environment.

No standalone installation, source overlays or global package changes. Source
tests and isolated children share the candidate wheel versions; Node tests use
the SDK lockfile rather than an arbitrary global TypeScript compiler.
"""
from __future__ import annotations

import argparse
from email.parser import BytesParser
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
import venv
import zipfile


def candidate_wheels(directory: Path, *, allow_dirty: bool = False) -> tuple[dict, list[Path]]:
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("format") != 1 or (manifest.get("dirty_snapshot") is not False and not allow_dirty):
        raise ValueError("Qualification requires a clean, format-1 source manifest")
    rows = manifest.get("wheels")
    if not isinstance(rows, list) or not rows:
        raise ValueError("The candidate wheel cohort is empty")
    paths, names = [], set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Invalid candidate wheel entry")
        name = row.get("file")
        if not isinstance(name, str) or Path(name).name != name or not name.endswith(".whl"):
            raise ValueError("A wheel must be a direct file in the candidate directory")
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("A candidate wheel is missing or symbolic")
        if hashlib.sha256(path.read_bytes()).hexdigest() != row.get("sha256"):
            raise ValueError("Candidate wheel digest mismatch: " + name)
        with zipfile.ZipFile(path) as archive:
            entries = [item for item in archive.namelist() if item.endswith(".dist-info/METADATA")]
            if len(entries) != 1:
                raise ValueError("Wheel metadata is ambiguous")
            metadata = BytesParser().parsebytes(archive.read(entries[0]))
        distribution = metadata["Name"]
        if distribution in names or distribution != row.get("name") or metadata["Version"] != row.get("version"):
            raise ValueError("Candidate metadata does not match its exact manifest")
        names.add(distribution)
        paths.append(path)
    return manifest, paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheels", required=True, type=Path)
    parser.add_argument("--environment", required=True, type=Path)
    parser.add_argument("--allow-dirty-source", action="store_true",
                        help="Development only: record, but do not qualify, an uncommitted build")
    options = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    directory = options.wheels.resolve()
    environment = options.environment.resolve()
    if environment.exists():
        parser.error("Use a new environment; existing environments are never modified")
    uv, npm = shutil.which("uv"), shutil.which("npm")
    if uv is None or npm is None:
        parser.error("Install uv and Node/npm in the build toolchain")
    manifest, wheels = candidate_wheels(directory, allow_dirty=options.allow_dirty_source)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if manifest["repository_commit"] != revision:
        parser.error("Source tests must match the candidate manifest commit")
    for name, digest in manifest["sources"].items():
        path = root / name
        if Path(name).is_absolute() or ".." in Path(name).parts or path.is_symlink() or not path.is_file():
            parser.error("Invalid or missing candidate source")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            parser.error("Source changed after wheel build: " + name)
    version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    venv.EnvBuilder(with_pip=True).create(environment)
    python = environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    def run(*command, cwd=root):
        subprocess.run([str(value) for value in command], cwd=cwd, check=True)
    run(uv, "pip", "install", "--python", python, "--find-links", directory,
        "-r", root / "packaging/requirements-test.txt", *wheels,
        f"openagent-core[engine,providers,audio,retrieval,modules]=={version}")
    run(uv, "pip", "check", "--python", python)
    # -I proves that installed entrypoints and children do not borrow source
    # PYTHONPATH. The full test process may then use its normal source fixtures.
    probe = subprocess.check_output([str(python), "-I", "-c",
        "import importlib.metadata as m,json,openagent_core; "
        "print(json.dumps({'core':openagent_core.__version__, 'distributions':"
        "{d.metadata['Name']:d.version for d in m.distributions()}}))"], cwd=environment, text=True)
    installed = json.loads(probe)
    if installed["core"] != version:
        raise RuntimeError("The isolated worker does not use the candidate runtime")
    for row in manifest["wheels"]:
        if installed["distributions"].get(row["name"]) != row["version"]:
            raise RuntimeError("The installed optional module does not match its candidate")
    (directory / "installed-environment.json").write_text(json.dumps(installed, indent=2) + "\n")
    run(npm, "--prefix", root / "sdk/typescript", "ci", "--ignore-scripts", "--no-audit", "--no-fund")
    run(npm, "--prefix", root / "sdk/typescript", "test")
    run(python, "-m", "pytest", "tests/public", "tests/prompts", "-q",
        "--junitxml", directory / "public-engine-tests.xml")
    receipt = {"format": 1, "status": "development-tests-passed" if manifest["dirty_snapshot"] else "installed-engine-tests-passed",
               "repository_commit": revision, "core_version": version,
               "manifest_sha256": hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest(),
               "dirty_source": manifest["dirty_snapshot"], "dependency_check": "passed",
               "typescript": "locked SDK build, unit test and live gateway fixture passed",
               "python_suites": ["tests/public", "tests/prompts"],
               "models_or_live_channels_qualified": False}
    (directory / "engine-qualification.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
