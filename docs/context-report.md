# Context report

`openagent_core.core.context_report.build_context_report(agent, session_id)`
returns the complete context-window report for synchronous callers. Async hosts
use `await build_context_report_async(agent, session_id)` from the same module.
The report, token estimates, model-window lookup, recorded billing metrics and
empty/error behavior are identical. History is never truncated for reporting.

The async contract runs database reads and transcript tokenization through the
shared persistence worker. Runtime and execution ContextVars reach that worker;
cancellation drains admitted work before returning ownership to the caller.
It caches neither the report nor authorization. A host must recheck access
after awaiting a report and before publishing it. `SessionInspection` performs
both checks, and session commands and live stream reports use the async builder.

Catalog refresh on a cold static/fallback lookup remains a background task on
the host's event loop. The initial report uses the current catalog and retains
the existing fallback window; refresh does not delay that report. Catalog
response JSON decoding remains owned by the model discovery implementation.

Regression coverage is in `tests/public/test_async_context_report.py`: complete
report parity and freshness, event-loop availability, runtime context, worker
cancellation, empty/error fallbacks, lazy refresh and post-read authorization.
