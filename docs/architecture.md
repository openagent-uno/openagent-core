<!-- Concrete host labels in this historical record are anonymized; original bytes and hashes are retained in the protected operator audit. -->
# Embedding architecture

## Packages and products

`openagent-core` contains the runtime contracts and the existing agent engine.
Its minimum installation has no third-party dependencies and starts no services
on import or construction. The optional `engine`, `providers`, `modules`,
`retrieval`, and other extras are selected by the host. SQLite operational
storage and the generic capability host are separate distributions.

`openagent-tools` owns independent computer tools and sidecars. The core does not
import a computer tool or install a binary. A tool provider can be used without
the agent engine. The product monorepo `openagent` owns the standalone server,
app, CLI, MCP bridge, native identity, dashboards, configuration, installers,
updaters, and support integrations.

An embedding host imports pinned library distributions into its own worker.
Its control plane owns authentication, delivery, projections, connector policy,
resources, isolation, limits, credentials and persistence provisioning. Core does
not require a particular control-plane language or orchestration platform.
The runtime accepts resources already prepared by the host and has no Kubernetes
dependency. A minimal host can supply an external identity authority
and a fixed capability catalog without standalone product services.

## Lifecycle and ownership

Optional behavior follows the uniform descriptor and surface contract documented
in [modules-v1.1.md](modules-v1.1.md). The runtime receives an explicit
`RuntimeProfile`; installed wheels are inert until that profile selects them.

```python
runtime = Runtime(settings, services, modules)
await runtime.start()
try:
    accepted = await runtime.submit(request, verified_context)
    result = await runtime.wait(accepted.run_id, verified_context)
finally:
    await runtime.close()
```

A host may construct `RuntimeSettings(agent_id)` with no local workspace.
The kernel runs against the supplied store and executor. File-based services
must be explicitly provisioned with an absolute workspace. Inside a workspace-free
runtime, resolving default config, DB, or vault directories raises an error
instead of using the operating system user's home. File-backed executors and
modules still need their own storage configuration.

Construction does no I/O. `start` opens only explicitly owned storage and modules;
it recovers incomplete runs without repeating effects. Module startup must clean
up a partially opened module. `close` cancels its exact runs and closes only its
owned resources. `AgentExecutor` adapts the preserved algorithm to this service.
Provider selection, model catalog loading, builtin selection, identity setup,
dashboard migrations, voice factories, and warmups are product bootstrap work.

Per-agent paths, cancellation, compaction, credentials, hooks, vault services,
provenance, and child execution limits belong to the runtime instance. Host code
converts environment configuration to explicit settings. Provider constructors
receive their own credentials; they do not export keys into `os.environ`.

Hosted Team delegation uses `Runtime.spawn`, including delegation to all members.
The dispatcher binds each actual member object to the exact configured model
reference; the host's `ModelCatalog` reauthorizes and resolves it. Display labels,
legacy session metadata and provider arguments cannot select an identity or
recover a model reference. Missing bindings or mismatched execution contexts fail
before model I/O, with no legacy storage fallback. Child requests inherit the
parent's accepted attachment references, authority, audience, scopes and temporary
leases; the mission is authored by the host agent. The legacy member-session flag
does not disable canonical child sessions in a hosted Runtime.

`Runtime.spawn` derives depth and root from admitted run lineage and enforces
instance-local depth, per-depth concurrency and per-chain concurrency limits. The default depth is one: a main run may fan out to workers, but a worker cannot admit grandchildren. Workers use a single model with their normal authorized tools, without an implicit routing team. A host must deliberately opt into a deeper envelope.
Separate depth tiers allow a parent to wait for its child without exhausting that
child's slots. Authorization is rechecked after waiting for capacity, and parent
cancellation cancels the exact admitted child. Child failures remain failures.

## Identities, audience, and ingress

`PrincipalRef` contains `authority`, `tenant_id`, `subject_id`, and `kind`.
`ExecutionContext` separately records the immutable author, run initiator,
execution authority, audience, scopes, delegation, and temporary capability
leases. Only authenticated host code constructs this context. An HTTP body cannot
choose it. The core has no signup, password, user directory, or organization model.

The same session can receive multiple users' messages. Different author/context
pairs never merge into a turn or inherit each other's credentials. Children keep
the initiating authority while their author is the agent. Read, replay, execution,
tool output, and final publication are separately authorized against current
policy. Shared output requires authorization for its actual audience.

An authenticated host may preserve an external sender as `author` and
`initiator` while selecting a distinct channel-granted `authority` and an
explicit channel/session audience. Every principal still uses the same host
tenant; an external identity namespace does not permit cross-tenant execution.
The host owns sender verification and grant policy, including revisions and
revocation. Core carries that context through admission, tool invocation and
publication, and rechecks current policy for replay without rebinding the
original admission. The neutral regression
[`test_external_channel_authority.py`](../tests/public/test_external_channel_authority.py)
qualifies this existing public contract through Runtime, SQLite and a function
capability. It does not qualify a live channel, provider or released package.

Device capabilities are tied to a verified ingress instance and generation.
App dashboard registration is independent from the local-device consent. CLI and
channel turns do not acquire dashboard tools merely because an app is connected.
Disconnect, revocation, or generation replacement prevents new calls. A delayed
disconnect cannot revoke a newer lease. Synchronous children may inherit the
turn's lease; durable automation cannot retain it.

## One tool dispatcher

Discovery returns descriptive names, schemas, target labels, and opaque
`tool_ref` values. Calling a tool requires its exact reference. Source, target,
identity, and generation are registered by the host and cannot be replaced by
tool arguments. Two sources may expose the same name; execution never falls back
to another source. Full MCP content, structured output, errors, and metadata are
retained through the public dispatcher.

Product registrations are immutable through user management APIs. Mutable user
registrations are restored through a distinct trusted host path. REST, marketplace
imports, MCP manager tools, PTC, and workflow calls must use the same catalog and
management services. Runtime tool dispatch persists invocation intent before
executing the effect and persists its authorized result afterwards. An uncertain
or interrupted effect cannot be silently repeated.

Background job notifications contain a logical source, output tool, arguments,
and result summary. The core does not own the computer process. Queues are scoped
to the full execution context so another user/device cannot receive those results.

## Run authority and delivery

The host allocates `run_id` and `idempotency_key` before sending. Acceptance,
input author, terminal transitions, events, and invocation receipts use the
existing operational SQLite tables. The same key with different input or author
is a conflict. Disconnecting an observer does not cancel an accepted run.

Within a runtime, turns and deferred provider-session release actions share a
lock per tenant and session. The lock stays registered while an action holds
it or waits for it, and is reclaimed as soon as the last action leaves.
Session history remains available in the durable store after the lock is gone.

Cancellation targets an exact run. A host may reserve cancellation for an
authorized run whose HTTP acceptance is uncertain. The reservation uses the
existing immutable domain event log; it invents neither a message nor a session
owner. If the delayed request arrives, its real author and input are stored with
terminal cancelled status and no executor starts.

embedding-host's delivery record tracks attempts, leases, frozen input, acceptance,
and uncertainty. Its runtime projection consumes durable events with a cursor.
The 16-minute execution deadline belongs to the runtime; the acceptance request
has a short timeout. A failed HTTP read triggers reconciliation of the same run.
`cancel_requested` remains an intention until the runtime confirms the outcome.

`AgentExecutor` treats a terminal `run_stream` frame with `errored: true` as a
failed durable run. The frame's `error_public` may explain the failure to the
caller, but its `text` is never persisted as a successful assistant reply.
Hosts should use this public adapter for ordinary engine execution rather than
copying its stream loop into product code.

## Prompts and memory

Every agent execution composes the mandatory framework, applicable module rules,
host instructions, and verified dynamic context. The host system prompt cannot
replace framework blocks. Block revisions and hashes are recorded in each run;
stable and dynamic blocks have separate cache identities. Tool documentation is
not silently promoted to framework authority.

The inventory maps original tool and vault rules to explicit tests. Vault recall,
successful saves, note patches, deduplication, quality gates, links, rename,
frontmatter, dream child sessions, provenance, Git history, citations, and end-of-
turn reminders move together. The initial reminder and every-third-turn cadence
are preserved. Only successful writes from trusted module semantic effects count
as a save; a custom MCP cannot forge that effect. Read-only or restricted-audience
memory never imposes an unauthorized write requirement.

### Host-owned document vault

A host can bind a database-backed vault to a workspace-free runtime. Install
the engine dependencies and the vault module. Supply an async SQLAlchemy engine
for SQLite (aiosqlite) or PostgreSQL (psycopg 3), a trusted space resolver, and
the same authorizer used for the runtime.

```python
from openagent_core.memory.vault.documents import SqlDocumentVault
from openagent_core.modules import ModuleConfig, ServiceRegistry

vault = SqlDocumentVault(engine, authorizer, resolve_space)
await vault.initialize()  # provisioning, before workers start
registry = ServiceRegistry({SqlDocumentVault: vault})
vault_config = ModuleConfig(frozenset({"service", "agent_tools", "host_api"}),
                            {"backend": "documents"})
```

Pass the registry to `RuntimeServices` and select `vault_config` in the
runtime profile. The host owns engine shutdown. Resolve the space from verified
identity and grants on each call. One space is a single access domain within a
tenant and agent. Personal and group vaults need distinct space IDs. The
authorizer must evaluate the complete audience on `memory.publish` and the
initiator on `memory.read` or `memory.write` for a `vault-space` resource.

The module exposes Markdown CRUD, revisioned reads, full-text search, cursor
pagination and incoming links by full vault-relative path. Revision zero creates a
note. Edits require the observed revision. Forgetting erases all logical revisions
and search entries and retains a tombstone against stale writes. The host applies
its own backup retention. Active content, revisions, outgoing links and search
state change in the same transaction. Agent recall and federated search consult
the authorized document service. A local vault continues to use the files backend
and the existing Git and quality tools.

### Personal API

The HTTP gateway accepts an optional `resolve_personal_context(request, catalog)`
async context manager. Authenticate every request and yield a context for one
principal and that same audience. Its session ID is a routing context only.
Browsing creates no session or run. Pass `personal_prefix` to mount a custom
route. Python and TypeScript export `PersonalClient`.

* `GET /api/v1/me/sessions` lists active authorized conversations. Use `q` to
  search, `ref` for the exact session detail and `cursor` to continue.
* `GET /api/v1/me/memories` lists the document space selected by the host. Use
  `q` to search, `path` to read one exact Markdown document and `cursor` to
  continue an ordered list or search. Both catalogs support limits from 1 to 100.

### Scheduled document-vault dreams

The optional document worker sends a memory-maintenance turn through the
configured runtime executor. It reads and edits notes with the native vault
tools. The files backend keeps its existing standalone maintenance and Git
behavior. Document maintenance uses SQL for coordination and requires no
filesystem, browser, editor, or connected personal device.

Create a `DocumentDreamQueue` with the vault's SQLAlchemy async engine, pass
it as `dream_queue=` to `SqlDocumentVault`, then call `vault.initialize()`
at provisioning time. Construct `DocumentDreamWorker(queue, resolve_context,
vault.resolve_space)`, bind it under its type in the service registry and
select the vault surfaces `service`, `agent_tools` and `workers`. Omit the
worker surface on an API-only instance. A host-managed task pool can call
`worker.run_due_once(runtime)` directly. Each tick admits or observes at
most one eligible space. Configure fleet capacity, model account and token
budget through the host's normal admission policy.

Document changes mark their space dirty inside the same transaction. Writes
in one maintenance pass share a generation. A foreground edit or deletion
invalidates pending derived writes from that pass. The next pass observes
the new revision. Worker leases coordinate claims across hosts; after a lost
receipt a successor reconciles the same run ID through the runtime ledger.
Terminal failures and denied grants are recorded without replaying effects.
The next admitted change or explicit request can schedule another pass.
Supply a host `next_due(scope, now)` callback returning a Unix timestamp to
select maintenance windows or use the default delay after the first edit.
A manual `await queue.request_now(vault, interactive_context, request_id)`
checks the caller's grant and deduplicates the request. Additional manual
requests join an active pass. Foreground note mutations schedule a new pass.

The trusted `resolve_context(claim)` callback reconstructs a stable, live
delegation for `claim.tenant`, `claim.agent`, `claim.space` and
`claim.session_id`, with `claim.context_scope` in its scopes, the authorized
result audience, `deferred=True` and no interactive device leases. Return
only a context whose credential scope and tools are restricted to this
vault; apply that restriction in the host authorizer on every discovery,
call and publication. The worker checks the catalog at admission and
requires `memory.dream`, read, write and publication permission. Every vault
operation rechecks its bound space and live delegation. The maintenance
session records the engine's answer under the same audience. Note edits
retain the observed revision and run provenance in the vault history.

## Automations

The existing scheduling, timezone/DST, workflow, and delivery algorithms remain.
Definition mutations and delegation capture share one transaction through
`AutomationRepository` and the host management service. Enqueue/commit/poll
operations use a separate repository session so waiting cannot hold a write lock
against the scheduler.

An unattended operation uses an explicit revocable grant for a particular
definition digest/revision. `Runtime.execute_operation` admits a trusted host
executor through the same run authority. Its versioned definition is part of the
request fingerprint. Tokens are resolved in an ephemeral execution scope; they
are never stored in the context. Existing definitions need their actual historical
authority resolved during migration; the installation owner is never guessed.

## Standalone document management

The document administration adapter uses the same space resolution and
revision checks as agent tools. Opening a graph or running the quality gate
constructs a transient in-memory index for that authorized space. An ordinary
note deletion retains its revisions. The explicit forget operation also erases
the revision bodies. Concurrent writes using a stale revision return a conflict.

### Cloud attachment bytes

Bind `SqlObjectArtifacts(engine, S3Chunks(client, private_bucket), authorizer)`
to `attachments.publisher` after calling `initialize()`. The host owns the
async S3 client and SQLAlchemy engine. Uploads stream into bounded chunks with
SHA-256 integrity checks. Content keys are namespaced by tenant and agent. One
SQL row publishes the ordered manifest and binds the artifact to its producing
run, session and recipient list. The byte store stays private after a failed
upload. Retention procedures may collect unreferenced chunks after in-flight
uploads have drained. During delivery the reader checks authorization and
integrity for each chunk. Set the gateway's `resolve_artifact_context` callback
to serve the artifact content URL using the caller's current verified session.
