"""Usage ledger — what the subscription is actually being spent on.

Token counts alone do not answer the question that matters for a subscription
("how much of the plan did that cost?"), because the plan is not billed in
tokens. The Codex edge reports a percentage of the window on every response, so
this ledger keeps both: token totals per model, and the plan percentage the
account had consumed at the start and end of each day. The difference between
those two percentages is the only honest measure of burn.

Persisted as JSON next to the credential store so a restart does not erase the
history, and bucketed per UTC day so it cannot grow without bound.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional


def _today() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


def _empty_counters() -> Dict[str, Any]:
    return {
        "turns": 0,
        "input_tokens": 0,
        "cached_tokens": 0,
        "output_tokens": 0,
        "reasoning_tokens": 0,
    }


class UsageLedger:
    def __init__(self, path: Path, *, retain_days: int = 30):
        self.path = Path(path)
        self.retain_days = max(1, retain_days)
        self._lock = threading.Lock()
        self._data: Optional[Dict[str, Any]] = None
        self._dirty_since_flush = 0
        self._last_flush = 0.0

    # -- persistence -------------------------------------------------------
    def _load_unlocked(self) -> Dict[str, Any]:
        if self._data is not None:
            return self._data
        data: Dict[str, Any] = {"version": 1, "days": {}}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text())
                if isinstance(loaded, dict) and isinstance(loaded.get("days"), dict):
                    data = loaded
            except Exception:
                pass
        self._data = data
        return data

    def _flush_unlocked(self, data: Dict[str, Any]) -> None:
        days = data.get("days") or {}
        for stale in sorted(days.keys())[: max(0, len(days) - self.retain_days)]:
            days.pop(stale, None)
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            tmp.replace(self.path)
        except Exception:
            # Accounting must never take the proxy down with it.
            pass

    # -- recording ---------------------------------------------------------
    def record(
        self,
        *,
        account_id: str,
        account_name: str,
        model: str,
        usage: Optional[Dict[str, Any]],
        quota: Optional[Dict[str, Any]] = None,
        kind: str = "chat",
    ) -> None:
        day_key = _today()
        with self._lock:
            data = self._load_unlocked()
            day = data["days"].setdefault(
                day_key,
                {"models": {}, "accounts": {}, "kinds": {}},
            )

            bucket = day["models"].setdefault(model or "?", _empty_counters())
            account_bucket = day["accounts"].setdefault(
                account_id or "?", {"name": account_name, **_empty_counters()}
            )
            day["kinds"][kind] = int(day["kinds"].get(kind, 0)) + 1

            for target in (bucket, account_bucket):
                target["turns"] = int(target.get("turns", 0)) + 1
            if usage:
                details_in = usage.get("prompt_tokens_details") or {}
                details_out = usage.get("completion_tokens_details") or {}
                deltas = {
                    "input_tokens": int(usage.get("prompt_tokens") or 0),
                    "cached_tokens": int(details_in.get("cached_tokens") or 0),
                    "output_tokens": int(usage.get("completion_tokens") or 0),
                    "reasoning_tokens": int(details_out.get("reasoning_tokens") or 0),
                }
                for target in (bucket, account_bucket):
                    for key, value in deltas.items():
                        target[key] = int(target.get(key, 0)) + value

            # Plan burn: remember where the window stood the first and the last
            # time we saw it today. `used_end - used_start` is the real cost of
            # the day's work, in the unit the subscription is actually metered.
            if quota:
                used = quota.get("primary_used_percent")
                if used is not None:
                    account_bucket.setdefault("plan_used_percent_start", used)
                    account_bucket["plan_used_percent_end"] = used
                secondary = quota.get("secondary_used_percent")
                if secondary is not None:
                    account_bucket.setdefault("plan_secondary_used_percent_start", secondary)
                    account_bucket["plan_secondary_used_percent_end"] = secondary
                if quota.get("plan"):
                    account_bucket["plan"] = quota["plan"]

            self._dirty_since_flush += 1
            # Flushing on every turn would fsync-thrash a busy proxy. Every ten
            # turns OR every 30s, whichever comes first — the timer matters
            # because a quiet proxy would otherwise never write its first nine
            # turns to disk, and `codex-sub-proxy usage` reads the file.
            now = time.time()
            if self._dirty_since_flush >= 10 or (now - self._last_flush) >= 30:
                self._dirty_since_flush = 0
                self._last_flush = now
                self._flush_unlocked(data)

    def flush(self) -> None:
        with self._lock:
            data = self._load_unlocked()
            self._dirty_since_flush = 0
            self._last_flush = time.time()
            self._flush_unlocked(data)

    # -- reading -----------------------------------------------------------
    def report(self, days: int = 7, *, include_days: bool = True) -> Dict[str, Any]:
        with self._lock:
            data = self._load_unlocked()
            keys = sorted((data.get("days") or {}).keys())[-max(1, days):]
            selected = {k: data["days"][k] for k in keys}

        totals = _empty_counters()
        by_model: Dict[str, Dict[str, Any]] = {}
        by_account: Dict[str, Dict[str, Any]] = {}
        for day in selected.values():
            for model, counters in (day.get("models") or {}).items():
                target = by_model.setdefault(model, _empty_counters())
                for key in totals:
                    target[key] += int(counters.get(key, 0))
                    totals[key] += int(counters.get(key, 0))
            for account_id, counters in (day.get("accounts") or {}).items():
                target = by_account.setdefault(
                    account_id, {"name": counters.get("name", account_id), **_empty_counters()}
                )
                for key in _empty_counters():
                    target[key] += int(counters.get(key, 0))
                start = counters.get("plan_used_percent_start")
                end = counters.get("plan_used_percent_end")
                if start is not None and end is not None:
                    # Windows reset, so a negative day is a reset, not a refund.
                    target["plan_percent_burned"] = round(
                        float(target.get("plan_percent_burned", 0.0)) + max(0.0, float(end) - float(start)), 2
                    )
                if counters.get("plan"):
                    target["plan"] = counters["plan"]

        report = {
            "window_days": len(selected),
            "from": keys[0] if keys else None,
            "to": keys[-1] if keys else None,
            "totals": totals,
            "by_model": by_model,
            "by_account": by_account,
        }
        if include_days:
            report["by_day"] = selected
        return report
