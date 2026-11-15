# Compaction summary fallback

Compaction folds older runs into a recap with a single summary call
(`OPENAGENT_COMPACTION_MODEL`, else the cheapest enabled api-based row). That call is
bounded by `OPENAGENT_COMPACTION_SUMMARY_TIMEOUT_SECONDS` (default 45). When it fails,
the session must not simply stay uncompacted, so a bounded list of fallbacks is tried
first. Which fallbacks are eligible depends on the kind of failure:

| Failure | Shared by the whole provider? | Eligible fallbacks |
| --- | --- | --- |
| quota, usage or budget exhausted, rate limit, authentication, "no available … accounts" | yes | another provider only |
| timeout, connection or another call-specific error | no | another model of the same provider, then another provider |

Order, never repeating a model and at most two fallback calls (each can take the full
summary timeout on the turn's maintenance path):

1. `configured`: `OPENAGENT_COMPACTION_FALLBACK_MODEL`, when it names an enabled
   api-based row, **whatever its provider**; it is the operator's explicit choice.
2. `same_provider`: for a call-specific failure only, another enabled model of the
   failed summariser's provider, cheapest by catalog pricing, ties in configuration
   order. On an install with a single working provider this is the only possible retry.
3. `other_provider`: a self-hosted row of another provider first, otherwise the
   cheapest row of another provider (never chosen by vendor name).

Events: each attempt emits `runtime.compaction.summary_fallback` with `reason`
(`configured`, `same_provider`, `other_provider`) and `fallback_model`. When nothing
was eligible, `runtime.compaction.summary_failed` reports `fallback="none_available"`;
when every fallback failed it reports `fallback="also_failed"` and `attempts`.
A skipped compaction then shows as `runtime.compaction.skipped` with `empty_summary`.

Operators who want a specific retry (for example a faster model of the same
subscription) set `OPENAGENT_COMPACTION_FALLBACK_MODEL=<provider>:<model>`.
