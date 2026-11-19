# Provider connections and built-in subscriptions

Core owns subscription OAuth, credential rotation, protocol translation,
admission, retry/cooldown and streaming for Claude and Codex. Install the
optional `openagent-core[engine,providers,subscriptions]` dependencies. The
kernel remains dependency-free and does not start listeners, import a CLI
account, read a keychain or select credentials from the process environment.
Hosts own authenticated management access, persistent storage and lifecycle.

## Connections, accounts and model kinds

A provider row is a **connection**, identified by its stable id and unique
`name`. Its metadata declares `vendor`, `access` (`api`, `subscription`,
`local`), a user-facing `label`, and, after subscription sign-in, `account_id`.
The name is a connection alias, not the vendor driver. Multiple API and
subscription connections to the same vendor can coexist.

Every model belongs to one connection. `models.kind` is `llm`, `image`, `tts`
or `stt`; the deprecated provider kind never excludes text models during
hydration. Only `llm` rows enter reasoning/team routing. Image and audio rows
retain their own existing capability paths. Each public model includes a
conservative `settings_schema`; hosts should render these fields instead of
assuming every adapter supports temperature, reasoning or output-token limits.
Existing unchanged overrides remain editable without destructive migration.

Runtime identities are `<connection-name>:<model-id>`. A model id may contain
slashes or effort suffixes; only a prefix matching the actual connection is
already qualified. Separate accounts therefore never share runtime identities
or sampling settings merely because they expose the same vendor model.

## Lifecycle and sign-in

`SubscriptionService('claude' | 'codex', absolute_credential_path)` is an
explicit host-owned service. `start_sign_in()` returns an authorization URL
and opaque expiring state; `complete_sign_in({state, code, name?})` accepts a
code or callback URL, validates state and persists the credential privately.
The PKCE verifier and tokens never appear in public account responses.

The common administration service exposes these operations to authorized
hosts through `subscription(context, provider_id, operation, fields)`.
Accounts are exact routing choices: `inference_client(account_id)` binds every
attempt to that account and never spends a sibling account's allowance.
Additional accounts use additional connections. Existing explicit credential
pools and external compatible endpoints remain supported separately; see
[provider pools and backups](provider-accounts-and-backups.md).

The protocol adapter runs through an in-memory streaming transport. It opens
no HTTP port and requires neither standalone proxy package nor executable.
Stream backpressure and cancellation reach the upstream producer. Hosts must
close the service after draining its calls; `Runtime.close()` closes services
created through its instance-owned registry.

Admission slots and start pacing are shared by workers using the same store;
starting another worker does not multiply an account's upstream concurrency.
OS locks release abandoned slots after a worker exits.

Rotating refresh tokens are protected by a thread and interprocess file lock.
Each operation reloads the private store under that lock before refreshing or
writing. Corrupt stores fail without overwriting their contents. Credentials
use atomic replacement and private files; hosts remain responsible for the
containing directory's ACL, particularly on Windows.

## Hosts with central credential custody

`SubscriptionService.from_access_token(provider, access_token, account_id=...,
admission_path=absolute_private_path, vendor_account_id=..., plan=...)` uses
an exact, ephemeral host-authorized grant. The host authenticates and authorizes
its caller, renews its account, and supplies the current access token. Core never
reads, copies or writes the host's credential store or refresh token. The
admission path coordinates nonsecret pacing and leases across workers; choose
one private path per actual account. Close each service after its calls drain.
Management mutations are refused for grant services.

The public `openagent_core.subscriptions.oauth.oauth` exchanges or refreshes
credentials through the same provider implementation as native accounts. The
host owns the authorize request, state verification, PKCE verifier, encrypted
custody and access policy. Supply `redirect_uri` and `refresh_scope` explicitly
when those differ from the native flow; this does not change native sign-in
permissions. Returned credentials belong only in the host's private custody API.
No browser, listener, token file or environment discovery is involved.

`openagent_core.subscriptions.errors.SubscriptionError` provides sanitized
`public()` and `headers()` projections. Quota windows retain their reset and
Retry-After without forwarding vendor messages or credential exchange bodies.
Grant inference requires a completed upstream terminal event. A truncated or
failed stream emits an error, never a fabricated finish or `[DONE]`. Native
Codex length-limited completions retain their established behavior. Hosts must
still preserve their authenticated HTTP admission, disconnect handling and
before-first-output error propagation when adapting the shared response.
`complete(body, account_id=...)` returns one validated result. `stream_chat(body,
account_id=...)` yields validated OpenAI SSE frames and raises a sanitized error
before yielding when the provider refuses the request. Prefetch its first frame
before committing a host HTTP 200. Close the iterator on downstream disconnect.
Raw upstream errors retain quota headers even for non-JSON error bodies; redirects
and process proxy settings do not redirect subscription credentials. Raw SSE
lines and total output are bounded, including malformed and ignored lines.

## Shared stores and migration

`subscription_for(provider_row, absolute_database_path)` resolves a private
store under the database's `subscriptions/` directory. An explicit
`credential_store_id` lets legacy text/image connections for the **same**
account share one rotating credential and service. Do not copy a rotating
credential into independent active stores.

`adopt_subscription_accounts(...)` is an offline migration: drain the runtime,
stop every old credential writer, take a verified cold backup, and supply the
unchanged source digest with `quiesced=True`. It preserves the original first
connection/model ids, runtime references, default preference, credentials,
endpoints, pins and history; sibling models receive distinct connection
identities. Original provider and model budget groups remain shared. For
multiple legacy connections using one pool, adopt the owner first and pass
its account-to-store mapping as `credential_owners` for subsequent rows.
Database-local provider ids may coincide across independent host databases.
Explicit `credential_owners` reuse validates the existing stopped-writer snapshot
instead of rejecting an id collision. A host may supply the same private
`subscriptions/` directory to its databases through its filesystem layout; all
workers then share the same OS locks and rotating store. This is a host-owned
storage arrangement, never an implicit search for another instance's credentials.
The source is preserved, and failures remove only newly created destinations.

A host rollback must merge the **latest** rotated credentials back into its
legacy store before restarting old writers. Restoring an old token backup
alone can invalidate access. Never run old and new writers concurrently.

## Declared account failover

Native connections remain bound to their named account. A host may declare an
`account_fallback_group` and integer `account_fallback_priority` on subscription
connections. Offline adoption preserves a legacy multi-account pool by declaring
this policy on the copied connections. `account_fallback_kinds` explicitly lists
`llm` and/or `image`; absent metadata retains the earlier text-only policy.
Adoption derives these kinds from the existing catalog capabilities. Pass `account_fallback=False` to adopt
exact connections without it. `account_fallback_enabled=False` removes a
connection from automatic selection while keeping its group configuration.

Only enabled, same-vendor, same-model entries in that group are candidates.
The requested account is tried first; siblings follow their declared priority.
The shared budget guard excludes exhausted candidate scopes. Each candidate
uses its own model settings and exact client. Only account failures (401, 403,
429, 529) before the first upstream frame may move to a sibling. Tools are not
replayed; cancellation, malformed requests and failures after any frame propagate.
Image generation uses `generate_subscription_image` with the same catalog and
budget admission. Only complete 401/403/429/529 refusals can select another
account; transport failures, invalid images and cancellation never replay a job.
Its result identifies the actual connection and account. Typed rows sharing a
credential are attempted once.

Stateless inference and connection probes still attest the exact account.
Reported request usage and effective-model attribution follow the account that
answered, with un-attributed run usage charged only once.

Hosts can pass validated nonsecret `subscription_settings` during adoption:
model aliases, advertised models, transport, deadlines, pacing and concurrency.
Private paths and credentials are not accepted in these catalog options. A
`subscription_admission_id` identifies one private shared admission lane for a
migrated pool, retaining its concurrency and pacing across separate rotating
account stores and processes. Hosts sharing a lane must use the same limits.
The optional settings `admission_path` on a directly constructed service is
host-owned; it contains only lease/pacing state. Grant services continue to
require their explicit admission path and never discover credentials.

## Usage and budgets

Quota is shown only when upstream reports it. Included subscription usage is
not a metered API charge; unknown pricing or missing usage must not be shown
as free capacity. Existing usage recording and enforcement coverage remains
capability-specific: routing caps govern reasoning models; an image/audio
adapter that does not report billable usage cannot provide a reliable money
meter or a hard spend cap. Hosts must communicate that limitation.

Provider scopes may target an exact connection, a declared `budget_group`, or
vendor. Cloned model scopes use `budget_model_group` to preserve inherited
caps. Exact prefix membership avoids wildcard collisions in connection names.
The SQLite ledger uses a provider/window covering index, and identical active
spend reads share execution without retaining completed results. A new recorded
usage write fences subsequent readers from the earlier snapshot. Borrowed
transactions retain the host's schema and query plan.

## Source provenance

The protocol implementations were adapted from the MIT-declared projects
[claude-sub-proxy at 1ed2f90](https://github.com/Spicy-Sparks/claude-sub-proxy/tree/1ed2f90d43ffad3d367bc54d22b74a8a07c69a36)
and [codex-sub-proxy at 812594d](https://github.com/Spicy-Sparks/codex-sub-proxy/tree/812594d298946bb1d2d496682fad092c99222472).
Core replaces their process-global configuration, HTTP-server lifecycle and
implicit local credential adoption with explicit instance-owned services.
The original Claude translation regressions are retained in Core. Repository
retirement belongs to operators after every installation has migrated.

## Explicit shared-custody HTTP host (beta136)

The normal connection remains in memory. An operator replacing a shared legacy
HTTP endpoint may explicitly start `python -m openagent_core.subscriptions.host`
from `openagent-core[subscription-host]`. It requires `--provider`, an absolute
`--credentials` path, an absolute nonsecret `--config` JSON path and `--port`.
The JSON accepts `settings` (the validated protocol options), `api_key_env`,
`admin_api_key_env`, and an optional absolute Codex `usage_file`. Key fields name
explicit environment variables; they do not embed secrets. No listener starts
on import or from creating a normal connection. The host must replace a drained
writer; do not duplicate a rotating token into several services.

From beta139, subscription-only hosts also keep the model package's reasoning
implementations lazy: constructing either subscription service and its HTTP app
requires the subscription extras, without the optional engine, database or model
SDK dependencies. Importing the model contracts or connection identity helpers
does not initialize a reasoning provider. Existing model class exports retain
their public names and load when explicitly requested.

Root `/v1/*`, health and separately authenticated administrative endpoints keep
the protocol adapter's compatibility behavior. Inference authorization never
implicitly grants credential administration. A key is required by default;
`--allow-unkeyed` explicitly preserves an already unkeyed endpoint behind its
existing network isolation. It is never inferred from a missing key.

`/subscription/accounts` returns a filtered nonsecret inventory and its reviewed
identity/configuration digest. `/subscription/models?account_id=...` uses that
exact account for authenticated catalog discovery. Exact inference is available
under `/accounts/<base64url-utf8-account-id>/v1/*`; a `/once/v1/*` variant leaves
pre-output account fallback to the connecting runtime. Neither path exposes
management or tokens. The host closes its service when its ASGI lifespan ends.

A connection with `metadata.subscription_remote=true` uses its explicit base URL
and inference key instead of creating a local token store. An unkeyed connection
requires the separate explicit `subscription_remote_unkeyed=true` flag. Account
routing, sampling and model identity are still local Core contracts. The logical
`credential_store_id` joins typed model rows for one remote account in that
catalog; it does not imply a token file on the connecting host.

`adopt_remote_subscription_accounts` requires a drained local runtime and the
`account_inventory_sha256` digest of reviewed ids, labels, enabled states and
priorities. Transient quota clocks are excluded. Original ids, pins, defaults
and budget grouping survive; no refresh material is copied or deleted. Other
HTTP consumers can keep the compatible root endpoint while they migrate.

`ProviderModelAdmin.subscription(..., 'update_account', ...)` changes local
connection enablement, label and fallback priority, including linked typed rows
in that database. It never changes the shared credential's enabled state or
label in another database. Explicit `SubscriptionService.update_account` remains
the credential-custody administration API. Account reads expose both effective
`enabled` and `credential_enabled`. The read-authorized `capabilities` operation
reports whether the configured endpoint permits browser sign-in; a connecting
host must honor that capability instead of advertising unsupported management.

The `ACCOUNT_CONNECTION_FIELDS` contract includes `fallback_enabled` for local
account policy updates. Administration applies it to all linked typed rows of
that account without changing the shared custody credential or other catalogs.
The optional HTTP custody host shares request metrics and the Codex usage ledger
between root and exact-account routes, flushing pending usage on shutdown.

### Retiring a custody listener without recreating connections

`localize_subscription_accounts(...)` converts an explicit reviewed list of
remote subscription provider IDs in place. Stop **all** credential writers and
consumers, verify the private catalog/credential backup, and supply the source
SHA-256, `quiesced=True`, a distinct positive `credential_owners` mapping for
all source accounts and a shared `admission_id`. No account or model row is
cloned. Connection/model IDs, enabled flags, defaults, settings, budget and
fallback groups, session references and unrelated API connections survive.
The obsolete endpoint is cleared; valid connection protocol overrides take
precedence over supplied host defaults. Even accounts without catalog rows
remain in their private stores. Results contain no credentials.

When separate databases use the same accounts, mount the same private
`subscriptions/` directory at each database directory and use the same owner
and admission identifiers. Individual rotating-file symlinks are refused:
atomic credential replacement would break their sharing. The underlying
filesystem must support cross-process file locking and atomic replacement;
a read-only mount or independently copied refresh token is not a replacement.
Existing destination accounts must exactly match the stopped source. A failed
transaction removes only destinations created by that invocation. Quiescence
and the reviewed source hash are mandatory; the function does not control
services or discover files. Only after every consumer has passed embedded
inference should the operator remove the retired listeners and their services.
Do not restore stale credential files when rolling back catalog rows.

### Streaming worker ownership

Blocking Codex streams run on a dedicated request worker. Acquiring, yielding
and closing a suspended protocol iterator on different pool threads can lose
the thread-local owner of its OS admission lock even after a successful reply.
The bounded bridge keeps `next()` and `close()` on the same worker, propagates
the host context and emits invisible SSE heartbeats. Cancellation stops queue
admission, pacing and retry waits, closes an established upstream response and
closes the iterator on its owning worker. The non-streamed aggregation path
uses the same request cancellation context. No account, model, quota or
concurrency setting is changed to recover a stranded lease.

### Quota telemetry without exposing credentials

Claude response quota headers are parsed inside the Core for both transports,
streaming and aggregation. Only allowlisted numeric windows/status fields and
an observation timestamp are persisted for the exact account and exposed by
`accounts()`. Request-scoped observers prevent cross-account contamination.
Ordinary replies remain usable if optional quota persistence is unavailable.

A trusted monitor can call `refresh_usage(account_id=..., model=...)` to send
a minimal one-output-token probe through that account's normal native admission
lane. This consumes a model request; schedule it deliberately. The result
contains the HTTP status, safe quota snapshot and `fresh` flag. Missing headers
remain unknown or stale, never a fabricated zero. Rate-limit replies can still
provide useful telemetry. Consumers keep thresholds, schedules, alerting and
other domain policy outside the Core; they do not read or transmit OAuth tokens,
reimplement provider headers or depend on a separate custody listener.

### Provider-scoped shared custody directories

`subscription_store_layout: "provider"` is an explicit native connection setting.
It stores credentials and admission state beneath `subscriptions/claude/` or
`subscriptions/codex/`, allowing the host to share only the required vendor's
directory with another process. The default `"flat"` retains every existing path.
No arbitrary path is accepted. Share directories, never individual rotating files.
The offline `localize_subscription_accounts(..., store_layout="provider")` operation
sets the layout while preserving reviewed provider/model IDs and settings. Stop
all writers first; never change an active connection's layout without migrating
its store. The host retains responsibility for mounts, permissions and custody.
