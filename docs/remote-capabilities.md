# Optional remote capabilities

Core defines contracts, not a device or account topology. A host may run with
zero clients, one or many clients, zero or many human principals, or non-human
principals. None of the adapters below is installed implicitly. The host owns
identity, current authorization, audience, enrollment and transport.

## Remote MCP sources

`openagent_core.remote_mcp.RemoteMcpRegistry(catalog, transport)` accepts a host
transport exposing `catalog(context, selectors)`, `call(context, scope, name,
arguments)` and `read(context, scope, uri)`. Catalog rows contain neutral
`host_id`, `generation`, `server`, `version`, `catalog_digest` and `tools` fields.
`resource_protocol: "mcp-app-resources/1"` is required only for resource reads.
Catalog limits are 256 sources, 512 tools/source, 4096 tools total and 2 MiB.

Call `await registry.synchronize(context)` before discovery. Its returned
snapshot can be used with `registry.discovery_scope(context, topology)` to avoid
repeating the full topology read for each source. Calls and resource reads
always revalidate the exact host, generation, server, catalog and principal /
session / audience before execution and before returning results. An incomplete
catalog retains previously known sources, but cannot prove they remain usable.
An unavailable topology publishes no source. Tool metadata and declared resource
URIs never authorize access or select a different source. Sources are bounded to
4096 registrations; hosts should bound the lifetime of their catalog instances.

Tool references keep the same public identity across model and app surfaces.
The MCP adapter resolves standard guest tool names within the exact originating
source, preserving established discovery and workflow names.

## Optional engine adapters

`ToolInventoryContext` exposes a bounded current tool result through an ephemeral
execution scope. The host selects the source/tool, context key, normalizer,
instructions and list key; Core interprets no domain identities. Deferred work
receives no interactive inventory. `AgentExecutor` accepts optional
`request_scope`, `command_handler` and `require_final_answer` arguments. The
request scope yields the resolved request; commands return a handled answer or
`None`. The original execution loop remains the sole engine implementation.

## Optional media

`CapabilityCatalog.stream_tool(tool_ref, options, context)` streams bounded JSON
packets from one exact optional executor. Core knows no video codec, desktop,
Electron, network protocol or user count. Authorization is checked before start
and before every published packet; catalog replacement, definition mutation and
lease revocation close the iterator. Closing it releases the producer. Sources
requiring canonical result/invocation provenance are denied until they provide a
streaming privacy publisher. A stream cannot substitute another executor.

`MCPPool.register_media_executor(server_name, executor)` is a trusted host API.
An executor provides `metadata(tool_name)` and an async `stream_tool` iterator.
Replacing it invalidates existing references. The host must qualify its transport
before registration. Ordinary MCP sources and no-client runtimes require no
media executor. `SessionInspection.participants` returns saved visible human
authors across the exact authorized session, independently of transcript pages;
authorship is descriptive and grants no membership or access.

## Task computers in embedded hosts

`openagent_core.computer_tasks.TaskComputerBroker` connects the existing
remote MCP registry to a host-provisioned machine. Importing the broker requires
SQLAlchemy asyncio and the driver for the host's SQLite or PostgreSQL engine.
An embedded API or worker can run on a read-only filesystem. Files, browser
profiles, shell processes and the capability host's state belong on the assigned
machine. The machine can reconnect while the runtime runs on another worker.

A `ComputerTask(project, task, base_version)` identifies an immutable starting
version. The allocation key includes the authenticated initiator, its identity
authority and tenant, the agent, project and task. Reusing the same task with a
different base fails. Supply a stable task for a persistent personal computer.
Supply distinct task IDs for parallel isolated workspaces forked from a project
version. The provider owns volume snapshots, writable overlays, browser profiles,
OS isolation, resource limits, cleanup and any explicit merge back into a project.

The host composes a broker with its async SQL engine, authorizer, provisioner,
and normal registered-device transport. The provider implements
`ensure(allocation)` and `connection(allocation, machine_id)`. Use the durable
`allocation.key` as the infrastructure idempotency key. Concurrent requests and
lost provisioning responses can invoke `ensure` again. Return the same unique
machine ID, preserve its data and never run a task on the API host. An interrupt
may leave infrastructure provisioned before its SQL receipt is written. The
provider reconciles it by allocation key. Destruction requires separate provider policy.

`connection` authenticates the machine's enrollment and returns
`ComputerConnection(machine_id, host_id, generation, expires_at)`. The host ID
remains bound to one allocation. Each new connection gets a strictly increasing
generation. The attested Unix expiration must fall within the configured
maximum lease duration. Refresh before expiry with `attach`. After expiry or
disconnect, the provider must attest a new generation. Call `disconnected(lease)`
when an exact registration is lost. Delayed disconnects cannot detach a successor.
`revoke(context, task)` permanently closes that allocation, including a pending
reservation.

```python
broker = TaskComputerBroker(engine, authorizer, provider, remote_transport,
                            delegations=live_delegations)
await broker.initialize()  # host provisioning
await broker.provision(verified_context, task)
lease = await broker.attach(verified_context, task)
async with broker.tools(runtime.capabilities, verified_context, lease) as bound:
    run = await runtime.submit(request, bound)
    result = await runtime.wait(run.run_id, bound)
```

Mount the tools before submitting the turn. Keep the mount until the run
finishes. Within that execution, the capability catalog contains only the
assigned target's registrations. Local worker tools, other connected machines
and references from other tasks are unavailable, including after disconnect.
The run's invocation receipts identify the enrolled host, connection generation
and catalog partition used for every device call.
Overlapping mounts of the same execution share their registrations. The final
mount releases them. An offline target or incomplete initial catalog prevents
admission. Any host command handlers that bypass the catalog remain the host's
responsibility.

Grant `computer.provision`, `computer.attach`, `computer.use`, `computer.publish`
and `computer.revoke` against the scoped `computer-task` resource as needed.
Separately authorize `computer.base.read` on `computer-project-version` using
the JSON pair `[project, base_version]` and the explicit result audience. This
permission is checked when cloning the base and whenever the derived machine
is used.
Use a fresh authenticated context on every entry. A deferred turn additionally
requires a verifiable live delegation and carries no interactive device lease.
A context's catalog partition selects the assignment and generation, and
the broker rechecks the live assignment, audience and policy on each call.
The device edge must call `await broker.admit_target(context, host_id,
generation)` at admission using its authenticated enrollment, then dispatch via
its normal per-principal capability host with its own consent, cancellation and
idempotency barriers. Preserve exact host, generation, server and catalog
digest across the transport. Revocation blocks new admissions and suppresses
late results. Calls already admitted by a target may have performed a mutation
and must be reconciled before retry.

The public broker suite exercises independent users and parallel tasks, lease
expiry, revocation, reattachment and a default worker tool on SQLite and
PostgreSQL. The optional device qualification uses two local capability hosts,
real filesystem/shell providers, two dedicated Chrome sidecars and a local
cookie page. It restarts both hosts, then checks each user's file, shell output
and browser cookie through new generations. It uses a deterministic model
boundary and fixture infrastructure on one OS account. A deployment qualifies
its own VM isolation, certified transport, cancellation and snapshot provider.

Release cohort: `1.1.0b48`. The public contract suite passed 720 tests and
97 subtests (two optional tests skipped). Package publication, host deployment
and real-device acceptance are recorded separately.

Hosts can pass explicit `unavailable_errors` classes to `ToolInventoryContext`
for optional transport failures. Those errors produce unavailable inventory;
other capabilities remain usable. Cancellation and arbitrary failures are not
swallowed, and Core imports no host transport or identity implementation.

## Direct desktop interaction

`openagent_core.computer_interaction.ComputerInteraction(runtime)` is an
optional, transport-independent service for an authenticated human desktop
viewer. The host binds a fresh `ExecutionContext` to the current user, agent
and session for every request. The service does not submit an agent run.
Use the same catalog registrations and authorization that already back the
underlying desktop tool.

- `await service.targets(context, source_id=None)` checks session read
  authorization and returns an array of `{tool_ref, label, live_media}` objects.
  Browser tabs and model-only tools are excluded.
- `await service.interact(context, body, source_id=None)` checks current
  session execution authorization and invokes exactly one current target.
  `body` contains `tool_ref`, `action` and the action's required `coordinate`
  or `text`. Successful calls return the normal tool result. Screenshot
  capture, pointer input, keyboard input and scrolling are supported.
- A configured `source_id` scopes both methods to one exact catalog
  registration identity. Pass it from host code, never from a viewer request.
  Hosts can select a private execution environment without authorizing
  registered personal devices, even if their tools have identical schemas.

A ToolRef identifies a capability but never authorizes it. Replacement,
revocation, and authorization changes invalidate access. The host still owns
agent and session admission, device consent, network transport, and policy for
a desktop whose contents may span multiple sessions. A host exposing these
methods over HTTP should return `Cache-Control: no-store`, keep screenshot
results out of logs, and avoid automatically retrying input after failures.
Live media metadata is descriptive. A host with a compatible media transport
must separately authorize and bind the exact stream.
