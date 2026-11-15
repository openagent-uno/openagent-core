"""Bounded previews of a run's text that keep the useful end.

An unattended run (scheduled task, event delivery) writes its narration first
("I'll start by reading…") and its conclusion last: the summary, the state of
the work, what is left. Stored previews are capped so a chatty turn cannot
bloat the database, but a head cut keeps exactly the part nobody needs and
drops the conclusion. These helpers keep the tail instead, with an explicit
marker saying how much earlier text was omitted. The full transcript stays in
the session history.
"""
from __future__ import annotations

# Large enough for a run's closing report (findings, state table, open items),
# small enough to stay a preview. Bounded per task by ``prune_task_runs``.
RUN_PREVIEW_CHARS = 8_000

_MARKER = "[… {n} earlier characters omitted; full transcript in the run's session …]\n"


def tail_preview(text: str | None, limit: int = RUN_PREVIEW_CHARS) -> str | None:
    """Return *text* unchanged when it fits, else its end within *limit* chars.

    The kept part starts at a line boundary when one is close, so the preview
    does not open mid-sentence, and is prefixed by a marker with the number of
    omitted characters. The result never exceeds *limit*.
    """
    if text is None:
        return None
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    marker = _MARKER.format(n=len(text))
    budget = limit - len(marker)
    if budget <= 0:
        return text[-limit:]
    tail = text[-budget:]
    newline = tail.find("\n")
    # Prefer a clean line start, but never give up more than a fifth of the budget.
    if 0 <= newline < budget // 5:
        tail = tail[newline + 1:]
    omitted = len(text) - len(tail)
    return _MARKER.format(n=omitted) + tail
