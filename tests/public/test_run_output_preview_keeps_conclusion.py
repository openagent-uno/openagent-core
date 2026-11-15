"""A stored run preview keeps the run's conclusion, not its opening narration.

Unattended runs narrate first and conclude last. ``task_runs.output`` and the
event-delivery output are bounded previews; cutting the head kept "I'll start
by reading…" and dropped the closing summary and state of the work.
"""
import asyncio
from types import SimpleNamespace

from openagent_core.core import event_dispatcher
from openagent_core.core.output_preview import RUN_PREVIEW_CHARS, tail_preview
from openagent_core.core.scheduler import Scheduler, _MAX_TASK_RUN_OUTPUT

NARRATION = "I'll start by reading the procedure note. " * 400  # ~17k chars
CONCLUSION = "## State\n| source | yes |\n| tested | yes |\n| pushed | no |\nDONE-MARKER"
LONG_RUN_TEXT = NARRATION + "\n" + CONCLUSION


def test_short_text_is_unchanged():
    assert tail_preview("short answer") == "short answer"
    assert tail_preview(None) is None


def test_long_text_keeps_the_end_with_a_marker_and_stays_bounded():
    preview = tail_preview(LONG_RUN_TEXT)
    assert len(preview) <= RUN_PREVIEW_CHARS
    assert preview.endswith("DONE-MARKER")
    assert "## State" in preview
    assert preview.startswith("[… ") and "earlier characters omitted" in preview


def test_small_limits_never_exceed_the_bound():
    for limit in (1, 10, 50, 120, 500):
        out = tail_preview(LONG_RUN_TEXT, limit)
        assert len(out) <= limit
        assert out.endswith(LONG_RUN_TEXT[-1])


def test_scheduler_records_the_conclusion_of_a_long_run():
    stored = {}

    async def update_task_run(run_id, **updates):
        stored.update(updates)

    async def prune_task_runs(task_id):
        return None

    fake = SimpleNamespace(db=SimpleNamespace(update_task_run=update_task_run, prune_task_runs=prune_task_runs))
    asyncio.run(Scheduler._record_task_finish(fake, "run-1", {"id": "task-1"}, status="success", output=LONG_RUN_TEXT))
    assert len(stored["output"]) <= _MAX_TASK_RUN_OUTPUT
    assert stored["output"].endswith("DONE-MARKER")
    assert "## State" in stored["output"]


def test_event_delivery_output_keeps_the_conclusion_without_a_retainer():
    out = event_dispatcher._retained_output(LONG_RUN_TEXT, agent=None)
    assert len(out) <= event_dispatcher.MAX_EVENT_OUTPUT_CHARS
    assert out.endswith("DONE-MARKER")
