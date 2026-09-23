# Symmetric module architecture

OpenAgent 1.1 separates the always-present kernel from optional domain modules.
The kernel owns verified identities, authorization, runtime lifecycle, durable
runs and events, the mandatory session substrate, capability dispatch, prompt
composition and module graph resolution. It has no table of product features and
does not activate a tool, vault, MCP server, workflow or worker by itself.

Every optional distribution exports a `ModuleDescriptor` with the same API.
`RuntimeProfile` selects modules and any combination of `service`, `agent_tools`,
`host_api`, `workers` and `event_ingress` surfaces. Descriptor resolution checks
installed packages, API versions, required modules and typed services before any
mutation. A contribution may bind typed services, capability sources, prompt
blocks, routes, health checks, search providers, workers, event ingress and
rebuildable indices. The host remains the only authority that can change a graph.

The independently built wheels are `openagent-module-sessions`, `-search`,
`-vault`, `-mcp`, `-workflows`, `-scheduler`, `-events`, `-delegation`, `-skills`,
`-models`, `-budget`, `-attachments`, `-logs`, `-ptc` and `-tool-discovery`.
`openagent-modules-full` is dependency-only. `openagent-modules` temporarily
contains compatibility implementations and assets while the beta aliases are
supported; installing either package does not activate a module.

Sessions are an ordinary optional administration module over the mandatory
`SessionStore`. It provides list, search, read, create, rename, archive, restore
and explicitly authorized purge. Vault is independent and contributes its own
tools, prompt rules, reminder and quality behavior only while active. Search
federates `SearchProvider` contributions; each domain still owns its canonical
search operation. The old conversation-search name remains a beta alias.

Deletion and recreation are separate generations even when a channel must reuse
a stable external id such as `tg:<user>`. A deleted compatibility source leaves
a tombstone and removes the previous transcript from active history. If the
verified host recreates that source, projection replaces the deleted generation
before the next run is admitted; old normalized runs, messages and tool rows do
not become visible again. An archived Sessions resource is unaffected and still
requires an explicit authorized restore.

MCP means external MCP protocol sources. Its fixed, product-managed and dynamic
catalog modes decide who may mutate those resources. Internal OpenAgent tools are
native module capabilities in the uniform `CapabilityCatalog`; the model always
calls an opaque `ToolRef` and never chooses a source, device or principal in tool
arguments. MCP catalog changes affect the next admitted run, while revocation is
checked on every invocation.

Workflows, Scheduler and Events have independent descriptors and domain workers.
The shared execution implementation activates only the domains present in the
profile: Scheduler can fire direct runs without Workflows, Workflows can process
manual requests without Scheduler, and Events can dispatch without either.
Integration resources are checked before hot deactivation. Active event targets,
workflow schedules and workflow references to configured external MCP sources
produce named conflicts; the host must cancel the change or pause/rewrite those
resources. Data is never deleted when a module is disabled.

`Runtime.reconfigure(profile, expected_generation=..., mode="drain")` validates
and prepares a new graph, swaps its generation atomically for new admissions and
drains the old run snapshot. `force` cancels runs tied to the retired graph.
Installed code never changes at runtime; adding or upgrading a wheel needs a new
artifact/process. Run events persist the profile generation, module versions and
surfaces, prompt revisions and capability generation used at admission.

The full standalone profile enables every domain. GlassPalace selects the same
modules per agent, supplies a federated Sessions backend and managed MCP catalog,
and owns pods and device capability sources. Replio demonstrates Sessions plus a
fixed native catalog with Vault, MCP, Scheduler, Workflows and Events absent.
