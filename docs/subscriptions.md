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
