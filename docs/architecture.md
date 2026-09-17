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

GlassPalace imports pinned library distributions into its own Python worker and
image. Its Go control plane owns authentication, delivery, projections, connector
policy, computers, pods, sandboxes, resource limits, credentials, and volumes.
The runtime accepts resources already prepared by the host and has no Kubernetes
dependency. Replio's reference consumer supplies an external identity authority
and a fixed capability catalog.

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

Cancellation targets an exact run. A host may reserve cancellation for an
authorized run whose HTTP acceptance is uncertain. The reservation uses the
existing immutable domain event log; it invents neither a message nor a session
owner. If the delayed request arrives, its real author and input are stored with
terminal cancelled status and no executor starts.

GlassPalace's delivery record tracks attempts, leases, frozen input, acceptance,
and uncertainty. Its runtime projection consumes durable events with a cursor.
The 16-minute execution deadline belongs to the runtime; the acceptance request
has a short timeout. A failed HTTP read triggers reconciliation of the same run.
`cancel_requested` remains an intention until the runtime confirms the outcome.

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
