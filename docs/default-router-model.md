# Default router model

A session without a model pin starts on the **default router**: the model
whose `models.is_classifier` flag is set (the column name predates this
meaning; no classifier call exists). Without one, the first enabled language
model in catalog order (provider name, framework, model) is used.

## Invariant

- At most one model carries the flag. Setting it on one model clears it on
  every other model in the same write.
- Only an enabled language model of an enabled provider can carry it.
  Disabling the model, or its provider, clears the flag; re-enabling does not
  restore it.
- `MemoryDB.set_model_is_classifier(id, True)` and an explicit
  `is_classifier: true` in `update_model_fields` refuse an ineligible model
  with `ValueError`. Bulk upserts store the flag only when the model is
  eligible.

SQLite triggers on `models` and `providers` enforce this in the statement
that writes, so the rule also holds for other writers of the same database
(the model-manager MCP, host SQL).

## Repair of existing state

Opening the database repairs state written by earlier releases: flags on
disabled models, on models of disabled providers, on non-language models and
duplicates are cleared. The row kept is the one the dispatcher already routed
to — the first eligible flagged model in catalog order — so the repair never
changes which model a deployment uses. A repair logs one warning naming the
kept and cleared model ids. A consistent database is not written to.

## Reading the effective default

`ProviderModelAdmin.default_model(context)` (`model.read`) returns the
public model row the runner starts unpinned sessions on, plus `reason`:
`"router_flag"` for the stored default, `"first_enabled"` when none is set, or
`None` when no language model is enabled. `MemoryDB.get_default_router()` is
the storage-level equivalent. Budget exclusions and session pins are applied
per turn and are not part of this answer.
