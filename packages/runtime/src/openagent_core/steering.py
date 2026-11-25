"""Model input for continuing unfinished work after an authenticated update."""
from __future__ import annotations

from collections.abc import Sequence


def continuation_input(previous: Sequence[str], current: str) -> str:
    unfinished = "\n\n".join(text for text in previous if text.strip())
    if not unfinished:
        return current
    return (
        "Continue the unfinished work using the latest instruction to adjust it. "
        "Keep earlier requirements that still apply and use completed tool results from the session. "
        "Avoid repeating actions that already succeeded.\n\n"
        f"Unfinished request\n{unfinished}\n\nLatest instruction\n{current}"
    )
