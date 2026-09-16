"""Re-run the preserved vault/tool baseline with isolated, offline fixtures.

Use an environment containing the optional engine/modules and standalone
gateway packages. Every case gets a new workspace, home, SQLite database and
empty provider configuration. No user configuration or credentials are read.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import importlib
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import traceback
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

MODULES = (
    "test_prompt_tool_names", "test_vault_reminder", "test_vault_gate",
    "test_vault_twins", "test_vault_contradiction", "test_vault_recall",
)


async def run(modules: tuple[str, ...]) -> list[dict]:
    import aiosqlite
    from legacy.tests._framework import TESTS, TestContext, TestSkip
    from openagent_core.core.paths import get_agent_dir, set_agent_dir
    from openagent_core.memory.vault.service import close_all
    from openagent_core.models import discovery

    rows = []
    # Import under the same isolated HOME used for execution; construction
    # paths cannot accidentally initialize an installed user's data directory.
    with tempfile.TemporaryDirectory(prefix="openagent-parity-") as suite_root:
        suite = Path(suite_root)
        env = {"HOME": str(suite), "PATH": os.environ.get("PATH", os.defpath),
               "TMPDIR": str(suite), "LANG": "en_US.UTF-8", "TZ": "UTC",
               "OPENAGENT_VAULT_VALIDATE_WRITES": "1"}
        previous = get_agent_dir()
        with patch.dict(os.environ, env, clear=True):
            set_agent_dir(suite)
            # The model fixture is entirely local. Populate the optional price
            # metadata cache so a fake model does not schedule an unrelated
            # public pricing fetch behind the deterministic stream test.
            discovery._OPENROUTER_CACHE = (time.time(), {})
            for module in modules:
                importlib.import_module("legacy.tests." + module)
            try:
                for index, (category, name, fn) in enumerate(TESTS):
                    if fn.__module__.rsplit(".", 1)[-1] not in modules:
                        continue
                    case = suite / f"case-{index:03d}"
                    case.mkdir()
                    set_agent_dir(case)
                    ctx = TestContext(case, {}, case / "openagent.yaml", case / "state.sqlite3")
                    ctx.config_path.write_text("{}\n")
                    conns = []
                    connect = aiosqlite.connect

                    def track_connection(*args, **kwargs):
                        conn = connect(*args, **kwargs)
                        conns.append(conn)
                        return conn

                    def deny_network(*args, **kwargs):
                        raise AssertionError("Parity fixtures must not connect to external services")

                    started = time.monotonic()
                    previous_tasks = asyncio.all_tasks()
                    row = {"category": category, "name": name, "test": fn.__name__}
                    try:
                        with patch.dict(os.environ, {**env, "HOME": str(case),
                                       "OPENAGENT_DB_PATH": str(ctx.db_path)}, clear=True), \
                             patch.object(aiosqlite, "connect", track_connection), \
                             patch.object(socket.socket, "connect", deny_network):
                            # Preserve the same ContextVar registry for cleanup;
                            # wait_for would move fn into a copied task context.
                            async with asyncio.timeout(120):
                                await fn(ctx)
                        row["status"] = "passed"
                    except TestSkip as error:
                        row.update(status="skipped", error=str(error))
                    except Exception:
                        row.update(status="failed", error=traceback.format_exc())
                    finally:
                        await close_all()
                        from openagent_core.core.logging import close_runtime_logging
                        close_runtime_logging()
                        for conn in reversed(conns):
                            await conn.close()
                        leftovers = asyncio.all_tasks() - previous_tasks
                        for task in leftovers:
                            task.cancel()
                        if leftovers:
                            await asyncio.gather(*leftovers, return_exceptions=True)
                    row["seconds"] = round(time.monotonic() - started, 3)
                    rows.append(row)
                    print(f"{row['status'].upper()}: {category}: {name} ({row['seconds']}s)", flush=True)
                    if row.get("error"):
                        print(row["error"], flush=True)
            finally:
                set_agent_dir(previous)
    return rows


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", type=Path)
    parser.add_argument("--modules", nargs="+", default=MODULES)
    args = parser.parse_args()
    results = asyncio.run(run(tuple(args.modules)))
    counts = dict(Counter(row["status"] for row in results))
    print(json.dumps({"counts": counts, "total": len(results)}, sort_keys=True))
    if args.json:
        args.json.write_text(json.dumps({"counts": counts, "tests": results}, indent=2) + "\n")
    raise SystemExit(0 if counts.get("passed") == len(results) else 1)
