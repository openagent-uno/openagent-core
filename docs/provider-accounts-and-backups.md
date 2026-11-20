# Provider accounts and backup models

Two public catalog contracts keep a conversation answering when a provider
account is out of quota or its credential stops working. Both follow the live
catalog: changing a provider or model row takes effect at the next catalog
rebuild, without restarting the runtime.

Built-in subscription connections use exact account routing; see
[subscriptions](subscriptions.md). The explicit API credential-pool contract
below remains available and does not implicitly pool independent cards.

## Several accounts for one provider

A provider row can carry a credential pool in `metadata.accounts`:

```json
{
  "accounts": [
    {"api_key": "first-key", "label": "work", "priority": 2},
    {"api_key": "second-key", "label": "backup", "priority": 1, "base_url": "https://other.example/v1"}
  ],
  "pool_strategy": "fill_first"
}
```

With two or more valid entries the runtime builds a pool
(`openagent_core.models.credential_pool`). `fill_first` (default) uses the
highest priority first; `round_robin` and `least_used` spread requests. An entry
without `base_url` inherits the provider's.

Before a turn moves to a backup model, a failure moves the same request to the
next account of the pool:

- **429 / 529** (rate limit, exhausted allowance): the account cools down for
  the provider's standard `Retry-After` (seconds, HTTP date, or
  `retry-after-ms`), bounded to 1 second … 31 days; without the header, one hour.
- **401 / 403** (rejected credential): the account is retired until the
  provider configuration changes, which builds a fresh pool. A host that signs
  an account in again should change that account's entry (any field, for
  example a `revision` counter) so the rebuilt pool uses it again.
- Anything else does not rotate and goes to the fallback path.

Streams rotate only before provider output; local request markers are excluded.
After provider output starts, a failure is surfaced without replay, so nothing
is duplicated.

`metadata.accounts[*].api_key` is write-only: provider rows returned by the
management API show `api_key_display` instead, like the provider's own key.

## Backup models

An enabled language model of an enabled provider joins the rate-limit and error
fallback chains when its model metadata has a non-negative integer
`fallback_priority`. Lower numbers are tried first, ties by runtime id. Refs
configured in the agent configuration (`fallback.on_rate_limit`,
`fallback.on_error`) keep precedence and are not repeated. Removing the key, or
disabling the model or its provider, removes it from the chain at the next
catalog rebuild; with no configured refs and no catalog backups there is no
explicit fallback chain. Automatic selection has the additional catalog
recovery policy described below.

The account pool is tried first; the backup models only when every account of
the failing provider is cooling down or retired.

A streaming backup that sends no provider output within
`OPENAGENT_FALLBACK_FIRST_OUTPUT_TIMEOUT_S` seconds (default 120) counts as
failed: its stream is closed and the next backup is tried. When none answers,
the turn ends with the original failure instead of waiting. This covers a
backup that accepts the connection and then never answers, such as an offline
machine behind a VPN or a stopped local server. The wait restarts after the
local markers that come before the request (request started, compaction). Once
output has started, the stream is never cut. Set the value to `0` to disable
the bound. It applies to async streaming backups, the path the runtime uses for
turns. The primary model and non-streaming calls keep their provider timeouts.

## Connection labels and account identity

`ProviderModelAdmin.update_provider(context, id, {"label": "Work"})` changes
the display label atomically while preserving stored metadata, pooled credentials,
provider identity and session model references. Labels contain 1 to 80 characters.
The connection contract advertises `PROVIDER_UPDATE_FIELDS`. Subscription account
responses include `account_name`, the credential account name, independently of
the connection's editable label and the existing compatibility `name` field.

## Automatic recovery (beta147)

The dispatcher's automatic selection adds enabled language models as recovery
candidates without changing session pins, configured backup chains or stored
settings. An explicitly selected model keeps its configured policy. Explicit
backup order comes first; additional automatic candidates prefer another
connection before another variant of the failing connection. Set model metadata
`auto_fallback=false` to exclude that row from additional automatic backups; primary selection and
explicit backup policy remain usable.

Each attempt rechecks `model.use`, input modalities, declared tool capability and
the current budget snapshot. Disabled connections/models, over-budget routes
and models in the temporary failure cooldown are skipped. A default 60-second
first-output deadline bounds automatic requests; SDK retries are disabled in
this lane because Core owns the retry and fallback policy. Manual requests keep
their provider settings. A provider failure never stops the Runtime service.

Automatic recovery covers quota/rate limits, connection/server failures,
rejected credentials and known unavailable-model responses. Malformed requests,
content refusals and arbitrary permission errors are surfaced. Local request
markers do not count as provider output. Once provider output or a tool has been
committed, Core does not replay the tool loop on another model. Non-streaming
calls also refuse recovery after a newly recorded tool call/result.

Reported usage is attributed to the model/connection that actually responded,
including recovery back to the original model later in one turn. Selection does
not invent tokens or a monetary charge. Existing account-pool policy remains a
separate, explicit same-model contract.
