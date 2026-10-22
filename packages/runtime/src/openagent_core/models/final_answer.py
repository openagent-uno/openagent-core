"""Authoritative completed-run answer boundary; no prose classification."""
from typing import Any


def is_parent_run_event(event: Any, runtime: Any, session_id: str) -> bool:
    """Only the actual root Agent/Team may attest this stream's answer."""
    identity = getattr(runtime, "id", None)
    if not isinstance(identity, str) or not identity:
        return False
    return (getattr(event, "session_id", None) == session_id
            and (getattr(event, "team_id", None) or getattr(event, "agent_id", None)) == identity)


def completed_final_answer(output: Any) -> dict[str, str] | None:
    """Use the completed run's terminal fresh, tool-free assistant message.

    Never search backwards: a tool/user terminal message, missing messages or
    history-only completion provides no attested answer. Aggregate run content
    may contain narration from earlier model requests and is not a substitute.
    """
    messages = getattr(output, "messages", None)
    if not isinstance(messages, (list, tuple)) or not messages:
        return None
    message = messages[-1]
    if (getattr(message, "role", None) != "assistant"
            or getattr(message, "from_history", False)
            or getattr(message, "tool_calls", None)):
        return None
    content = getattr(message, "content", None)
    if isinstance(content, str):
        return {"text": content}
    # Structured textual blocks are read by their provider type, never by
    # guessing which line or paragraph looks final. Unsupported content is
    # explicitly unknown rather than stringified into an invented answer.
    if isinstance(content, list) and all(
        isinstance(part, dict) and part.get("type") in {"text", "output_text"}
        and isinstance(part.get("text"), str) for part in content
    ):
        return {"text": "".join(part["text"] for part in content)}
    return None
