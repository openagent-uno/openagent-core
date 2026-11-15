# Run output previews

Scheduled-task runs (`task_runs.output` and `task_runs.error`) and event
deliveries store a bounded preview of the run's text; the full transcript stays
in the run's session history.

An unattended run narrates first ("I'll start by reading…") and concludes last
(findings, state of the work, what is left). The preview therefore keeps the
**end** of the text, not its beginning. When the text is longer than the limit,
the stored value starts with a marker such as
`[… 4663 earlier characters omitted; full transcript in the run's session …]`
followed by the closing part, cut at a line start when one is close.

The limit is `RUN_PREVIEW_CHARS` (8000 characters, in
`openagent_core.core.output_preview`), large enough for a closing report and
still a preview; the per-task history is bounded by `prune_task_runs`. An agent
extension that provides `output_retainer` still decides event-delivery previews
itself.
