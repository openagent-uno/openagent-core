from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator


class _Agent:
    model = None


def _guard(user_message: str, reply: str, rows: list[tuple[str, str]]) -> str:
    from openagent_core.core import reply_guard, tool_trace

    session_id = "reply-grounding-public-test"
    tool_trace.take(session_id)
    sink, token = tool_trace.maybe_open()
    try:
        assert sink is not None
        sink["tools"].extend(rows)
    finally:
        tool_trace.close(token)
    tool_trace.publish(session_id, sink)
    try:
        return asyncio.run(
            reply_guard.guard_reply(
                _Agent(), session_id, user_message, reply,
            )
        )
    finally:
        tool_trace.take(session_id)


def test_unbacked_authorization_error_is_not_sent() -> None:
    draft = (
        "Ho appena provato a leggere il Vault. "
        "La chiamata restituisce Not authorized for memory.read."
    )
    guarded = _guard("Ricordi il Patek?", draft, [])

    assert "Not authorized for memory.read" not in guarded
    assert "non lo presento come reale" in guarded


def test_real_authorization_error_can_be_reported() -> None:
    draft = "Ho provato ora: Not authorized for memory.read."
    guarded = _guard(
        "Leggi il Vault",
        draft,
        [("vault_read_note", "result=Not authorized for memory.read")],
    )

    assert guarded == draft


def test_attributed_user_report_is_not_misclassified_as_a_tool_claim() -> None:
    user = "Friday mi ha mostrato Not authorized for memory.manage"
    reply = (
        "L'errore che hai segnalato, Not authorized for memory.manage, "
        "va verificato con una chiamata reale."
    )

    assert _guard(user, reply, []) == reply


def test_empty_turn_clears_previous_receipt() -> None:
    from openagent_core.core import tool_trace

    session_id = "receipt-reset-public-test"
    first, first_token = tool_trace.maybe_open()
    try:
        tool_trace.record("vault_read_note", "ok")
    finally:
        tool_trace.close(first_token)
    tool_trace.publish(session_id, first)
    assert tool_trace.peek(session_id)

    empty, empty_token = tool_trace.maybe_open()
    tool_trace.close(empty_token)
    tool_trace.publish(session_id, empty)
    assert tool_trace.peek(session_id) is None


def test_streaming_batched_reply_uses_the_guarded_terminal_text() -> None:
    from openagent_core.core import tool_trace
    from openagent_core.core.agent import Agent
    from openagent_core.stream.session import StreamSession

    class _Model:
        history_mode = "caller"
        model = "test/model"

        def effective_model_id(self, _session_id: str | None = None) -> str:
            return self.model

        async def stream(
            self, _messages: list[dict[str, Any]], **_kwargs: Any,
        ) -> AsyncIterator[str]:
            yield (
                "Ho appena provato a leggere il Vault. "
                "La chiamata restituisce Not authorized for memory.read."
            )

        async def generate(self, *_args: Any, **_kwargs: Any):
            raise AssertionError("non-empty stream must not fall back to generate")

        async def close_session(self, _session_id: str) -> None:
            return None

    async def exercise() -> None:
        session_id = "stream-grounding-public-test"
        tool_trace.take(session_id)
        agent = Agent(
            name="grounding-test", model=_Model(), system_prompt="test", memory=None,
        )
        session = StreamSession(agent, client_id="owner", session_id=session_id)
        summary = await session.run_one_shot("Ricordi il Patek?", speak=False)
        assert "Not authorized for memory.read" not in summary["text"]
        assert "non lo presento come reale" in summary["text"]
        await agent.shutdown()

    asyncio.run(exercise())
