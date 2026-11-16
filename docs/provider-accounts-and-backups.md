# Provider accounts and backup models

Two public catalog contracts keep a conversation answering when a provider
account is out of quota or its credential stops working. Both follow the live
catalog: changing a provider or model row takes effect at the next catalog
rebuild, without restarting the runtime.

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

Streams rotate only before the first event; after output started, a failure
goes to the fallback path, so nothing is ever duplicated.

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
fallback chain at all.

The account pool is tried first; the backup models only when every account of
the failing provider is cooling down or retired.
