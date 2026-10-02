"""Public session operations for an already authorized host request.

The host authenticates the caller and checks access to the exact session before
using this service. Core owns the compaction lock and model eligibility rules;
hosts own their commands, HTTP routes, and authorization policy.
"""
from __future__ import annotations

from typing import Any


class SessionControl:
    def __init__(self, agent: Any) -> None:
        self.agent = agent

    @property
    def _db(self) -> Any:
        db = getattr(self.agent, "memory_db", None)
        if db is None:
            raise RuntimeError("Session storage is unavailable")
        return db

    async def compact(self, session_id: str, *, on_status: Any = None) -> dict[str, Any] | None:
        if not session_id:
            raise ValueError("session_id is required")
        from .core import compaction

        async with compaction.session_lock(session_id):
            return await compaction.compact(
                session_id, self.agent.model, self.agent, on_status=on_status, keep=0,
            )

    async def model_pin(self, session_id: str) -> str | None:
        if not session_id:
            raise ValueError("session_id is required")
        return await self._db.get_session_pin(session_id)

    async def available_models(self) -> list[dict[str, Any]]:
        return await self._db.list_models_enriched(enabled_only=True, kind="llm")

    async def pin_model(self, session_id: str, runtime_id: str) -> dict[str, Any]:
        if not session_id or not runtime_id:
            raise ValueError("session_id and runtime_id are required")
        row = await self._db.get_model_by_runtime_id(runtime_id)
        if (row is None or row.get("kind") != "llm" or not row.get("enabled")
                or not row.get("provider_enabled")):
            raise LookupError(f"Model {runtime_id!r} is unavailable")
        await self._db.pin_session_model(session_id, runtime_id)
        return row

    async def clear_model_pin(self, session_id: str) -> None:
        if not session_id:
            raise ValueError("session_id is required")
        await self._db.unpin_session_model(session_id)
