"""Explicit offline adoption of a host-owned legacy subscription credential store.

No discovery, refresh, service control or deletion. The host must drain its
runtime and stop the old credential writer before invoking this operation.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import time
import uuid
from pathlib import Path

from .credential_storage import write_credentials
from openagent_core.models.catalog import build_runtime_model_id


async def adopt_subscription_accounts(db, provider_id: int, provider: str, source: Path, *,
                                      quiesced: bool, expected_sha256: str,
                                      credential_owners: dict[str, int] | None = None) -> dict:
    """Split a legacy pool into exact account connections, preserving original IDs.

    The first account retains the provider/model IDs, pins, budgets and history.
    Sibling connections get copied model settings and no default-router flag.
    The source file is preserved byte-for-byte for the host's rollback plan.
    """
    if not quiesced:
        raise ValueError("Drain the runtime and stop the old credential writer first")
    if provider not in {"claude", "codex"} or not source.is_absolute() or source.is_symlink():
        raise ValueError("An explicit regular credential store and subscription provider are required")
    original = source.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    if digest != expected_sha256:
        raise ValueError("The credential store changed after the host stopped its writer")
    manager = importlib.import_module(f"openagent_core.subscriptions.{provider}.auth").TokenManager(source)
    accounts = manager.list_accounts()
    if not accounts:
        raise ValueError("The legacy credential store contains no managed accounts")
    # Keep historical references on the first account the legacy pool could
    # actually use, rather than silently binding them to a disabled account.
    now_ms = time.time() * 1000
    accounts.sort(key=lambda account: (account.get("enabled") is False,
                                       int(account.get("limited_until_ms") or 0) > now_ms))
    default_before = await db.get_default_router()
    target_root = Path(db.db_path).parent / "subscriptions"
    created = []
    adopted = []
    vendor = "anthropic" if provider == "claude" else "openai"
    try:
        async with db._isolated_write() as conn:
            raw = await (await conn.execute("SELECT * FROM providers WHERE id=?", (provider_id,))).fetchone()
            if raw is None:
                raise LookupError("Provider not found")
            legacy = dict(raw)
            metadata = json.loads(legacy.get("metadata_json") or "{}")
            if metadata.get("access") == "subscription":
                raise ValueError("This connection is already a built-in subscription")
            models = [dict(row) for row in await (await conn.execute("SELECT * FROM models WHERE provider_id=?", (provider_id,))).fetchall()]
            for index, account in enumerate(accounts):
                label = str(account.get("name") or account["id"])
                meta = {**metadata, "vendor": vendor, "access": "subscription", "label": label,
                        "account_id": str(account["id"]), "budget_group": legacy["name"]}
                pid = provider_id
                name = legacy["name"]
                if index:
                    name = f"{vendor}-subscription-{uuid.uuid4().hex[:16]}"
                    values = {**legacy, "name": name, "metadata_json": json.dumps(meta),
                              "enabled": int(legacy["enabled"] and account.get("enabled") is not False),
                              "created_at": time.time(), "updated_at": time.time()}
                    values.pop("id")
                    columns = list(values)
                    result = await conn.execute(f"INSERT INTO providers ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", tuple(values.values()))
                    pid = result.lastrowid
                    for model in models:
                        copy = {**model, "provider_id": pid, "is_classifier": 0}
                        copy.pop("id")
                        model_meta = json.loads(copy.get("metadata_json") or "{}")
                        original_runtime_id = build_runtime_model_id(legacy["name"], copy["model"])
                        for separator in (":", "/"):
                            prefix = f"{legacy['name']}{separator}"
                            if copy["model"].startswith(prefix):
                                copy["model"] = copy["model"][len(prefix):]
                                break
                        copy["metadata_json"] = json.dumps({**model_meta, "budget_model_group": original_runtime_id})
                        columns = list(copy)
                        await conn.execute(f"INSERT INTO models ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", tuple(copy.values()))
                else:
                    await conn.execute("UPDATE providers SET metadata_json=?,enabled=?,updated_at=? WHERE id=?",
                                       (json.dumps(meta), int(legacy["enabled"] and account.get("enabled") is not False), time.time(), pid))
                owner_id = (credential_owners or {}).get(str(account["id"]), pid)
                meta["credential_store_id"] = owner_id
                await conn.execute("UPDATE providers SET metadata_json=? WHERE id=?", (json.dumps(meta), pid))
                target = target_root / f"{owner_id}-{provider}.json"
                if credential_owners:
                    if owner_id == pid or not target.is_file():
                        raise ValueError("The shared credential owner must be adopted first")
                    existing = json.loads(target.read_text())
                    if existing.get("accounts") != [account]:
                        raise ValueError("The shared credential changed; do not duplicate a rotating token")
                else:
                    if target.exists():
                        raise ValueError("The destination credential store already exists; nothing was overwritten")
                    write_credentials(target, {"version": 2, "accounts": [account]})
                    created.append(target)
                adopted.append({"provider_id": pid, "connection_name": name, "models": len(models),
                                "account_id": str(account["id"]), "credential_store_id": owner_id})
            if default_before is not None and default_before.get("reason") == "first_enabled":
                await conn.execute("UPDATE models SET is_classifier=1 WHERE id=?", (default_before["id"],))
            if source.read_bytes() != original:
                raise ValueError("The source writer was not stopped; migration rolled back")
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return {"connections": adopted, "source_sha256": digest, "source_preserved": True,
            "original_provider_id": provider_id, "pins_preserved": True}
