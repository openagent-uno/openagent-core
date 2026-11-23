"""Aggregate migration inventory for a verified, host-fenced SQLite snapshot."""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess

from openagent_core.memory.operational.privacy_snapshots import guard_snapshot_transaction
from openagent_core.memory.operational.repository import projection_coverage

from .migration import (MANIFEST, _check_exact_tenants, _check_transaction_tenant, _digest, _relative,
                        _tenant_selection, verify_snapshot)


def _git_read(repository: Path, *args: str) -> bytes:
    command = [
        "git", "--no-optional-locks", "--no-replace-objects",
        "-c", "gc.auto=0", "-c", "core.commitGraph=false", *args,
    ]
    env = {
        "PATH": os.defpath, "GIT_DIR": str(repository),
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_ALLOW_PROTOCOL": "", "GIT_NO_LAZY_FETCH": "1",
    }
    result = subprocess.run(command, env=env, cwd=repository.parent,
                            capture_output=True, timeout=300)
    if result.returncode:
        raise ValueError("Vault history could not be verified")
    return result.stdout


def _vault_inventory(root: Path, manifest: dict, relative: str, git_history: bool, *,
                     history_only: bool = False) -> dict:
    if history_only:
        if not git_history:
            raise ValueError("Choose a single vault inventory mode")
        if manifest.get("format") != 2 or manifest.get("vault") != {"status": "absent", "path": str(_relative(relative))}:
            raise ValueError("An exact fenced source absence receipt is required")
        return {"status": "absent", "markdown_files": 0, "markdown_tree_sha256": None,
                "git_commits": None, "git_history_sha256": None}
    if manifest.get("vault") is not None:
        raise ValueError("A history-only snapshot requires explicit selection")
    vault = root / _relative(relative)
    if not vault.is_dir():
        raise ValueError("Declared vault missing from snapshot")
    notes, digest = 0, hashlib.sha256()
    for name, entry in sorted(manifest["files"].items()):
        path = root / name
        if not path.is_relative_to(vault):
            continue
        rel = path.relative_to(vault)
        if ".git" in rel.parts or path.suffix.lower() != ".md":
            continue
        digest.update(os.fsencode(rel) + b"\0" + bytes.fromhex(entry["sha256"]))
        notes += 1
    report = {"markdown_files": notes, "markdown_tree_sha256": digest.hexdigest(),
              "git_commits": None, "git_history_sha256": None}
    git_dir = vault / ".git"
    if not git_dir.exists() and not git_history:
        return report
    if not git_dir.is_dir() or any((git_dir / item).exists() for item in
            ("commondir", "shallow", "objects/info/alternates", "objects/info/http-alternates")):
        raise ValueError("A complete, self-contained vault history is required")
    if any((git_dir / "objects/pack").glob("*.promisor")):
        raise ValueError("Vault history requires external objects")
    _git_read(git_dir, "fsck", "--full", "--no-dangling")
    commits = sorted(_git_read(git_dir, "rev-list", "--all", "--reflog").splitlines())
    report["git_commits"] = len(commits)
    report["git_history_sha256"] = hashlib.sha256(b"\n".join(commits)).hexdigest()
    return report


def inspect_migration_source(snapshot: str | Path, database: str, vault: str, *,
                             expected_tenant: str | None = None, expected_tenants=None, require_git_history: bool = True,
                             history_only: bool = False) -> dict:
    """Inspect an existing verified snapshot and disclose aggregates only.

    This is source inventory. A destination still requires semantic verification
    and restore qualification before promotion.
    """
    root = Path(snapshot).resolve()
    manifest_digest = _digest(root / MANIFEST)
    manifest = verify_snapshot(root)
    selected = _tenant_selection(expected_tenant, expected_tenants)
    return _inspect_verified_source(root, database, vault, selected, require_git_history,
                                    manifest, manifest_digest, history_only=history_only)


def _inspect_verified_source(root, database, vault, expected_tenant, require_git_history, manifest, manifest_digest,
                             *, history_only=False):
    name = str(_relative(database))
    if name not in manifest["databases"] or manifest["files"][name]["kind"] != "sqlite":
        raise ValueError("Snapshot database must be declared")
    if not manifest.get("fence_id") or any(name + ext in manifest["files"]
                                           for ext in ("-wal", "-shm", "-journal")):
        raise ValueError("Fenced SQLite backup required")
    vault_state = _vault_inventory(root, manifest, vault, require_git_history, history_only=history_only)
    issues = []
    with closing(sqlite3.connect((root / name).as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        guard_snapshot_transaction(db)
        declaration = manifest.get("tenant_domains", {}).get(name)
        if isinstance(expected_tenant, tuple):
            if declaration != list(expected_tenant):
                raise PermissionError("Exact tenant inventory requires a matching host-sealed source declaration")
            _check_exact_tenants(db, expected_tenant)
        elif declaration is not None:
            raise PermissionError("Sealed source requires its complete tenant inventory")
        else:
            _check_transaction_tenant(db, expected_tenant)
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        required = ("sessions_v2", "session_runs", "session_messages", "tool_invocations",
                    "domain_events", "search_outbox", "artifacts")
        if not set(required).issubset(tables):
            raise ValueError("Snapshot lacks operational history tables")
        counts = {table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in required}
        counts["tombstoned_sessions"], counts["quarantined_sessions"] = db.execute(
            "SELECT COUNT(*) FILTER (WHERE deleted_at_ms IS NOT NULL), "
            "COUNT(*) FILTER (WHERE visibility='quarantined') FROM sessions_v2"
        ).fetchone()
        counts["unfinished_runtime_runs"] = db.execute(
            "SELECT COUNT(*) FROM session_runs WHERE finished_at_ms IS NULL "
            "AND json_extract(metadata_json,'$.runtime_contract')=1"
        ).fetchone()[0]
        for table in ("oa_document_run_sources", "oa_vault_read_receipts"):
            if table in tables:
                counts[table] = db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if counts.get("oa_document_run_sources"):
            # The native PG projection does not yet bind the same destination
            # vault revision lifetimes to its session, run and audience readers.
            issues.append("runtime_document_lineage_projection_required")
        if history_only:
            document_tables = {"oa_vault_notes", "oa_vault_revisions", "oa_vault_read_receipts",
                               "oa_vault_links", "oa_vault_derivations", "oa_vault_import_origins"}
            if any(db.execute(f'SELECT 1 FROM "{table}" LIMIT 1').fetchone() is not None
                   for table in tables & document_tables):
                issues.append("database_vault_requires_separate_migration")
        if counts["unfinished_runtime_runs"]:
            issues.append("unfinished_runtime_runs")
        if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
            issues.append("invalid_foreign_keys")
        legacy = None
        if "sessions" in tables:
            legacy = projection_coverage(db)
            legacy["missing_live_projections"] = db.execute(
                "SELECT COUNT(*) FROM sessions s WHERE NOT EXISTS "
                "(SELECT 1 FROM sessions_v2 v WHERE v.id=s.session_id AND v.deleted_at_ms IS NULL)"
            ).fetchone()[0]
            if not legacy["complete"]:
                issues.append("legacy_projection_incomplete")

    if _digest(root / MANIFEST) != manifest_digest:
        raise ValueError("Snapshot manifest changed during inspection")
    return {"format": 1, "scope": "source_inventory", "snapshot_manifest_sha256": manifest_digest,
            "database_sha256": manifest["files"][name]["sha256"], "counts": counts,
            "legacy": legacy, "vault": vault_state, "issues": issues}


def _tenant_cli(values):
    if len(values) == 1:
        return {"expected_tenant": values[0]}
    return {"expected_tenants": values}


def _vault_options(parser):
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--history-only", action="store_true",
                       help="Require a host-sealed absent vault at the exact --vault path")
    group.add_argument("--allow-unversioned-vault", action="store_true")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot")
    parser.add_argument("--database", required=True)
    parser.add_argument("--vault", required=True)
    parser.add_argument("--expected-tenant", required=True, action="append")
    _vault_options(parser)
    args = parser.parse_args(argv)
    try:
        report = inspect_migration_source(args.snapshot, args.database, args.vault,
                    **_tenant_cli(args.expected_tenant), require_git_history=not args.allow_unversioned_vault,
                    history_only=args.history_only)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.SubprocessError):
        print(json.dumps({"error": "snapshot_preflight_rejected"}))
        return 2
    print(json.dumps(report, sort_keys=True))
    return 1 if report["issues"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
