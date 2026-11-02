# Agnostic architecture and contribution rules

**User decision: 2 November 2026. Applies to every OpenAgent repository**, including
Core, Tools, the standalone product, templates, documentation mirrors and historical
release executors. OpenAgent is general-purpose software, independent of the
applications and clients that embed it.

## No consumer identity in OpenAgent

OpenAgent must not contain references to embedding projects or external consuming
clients: no consumer names, deployments, domains, tenant identifiers, private paths,
brand assets, bespoke feature flags or branches selected by a consumer's identity.
This applies to implementation, configuration defaults, prompts, examples, tests
and maintained documentation. Use neutral fixtures such as `host-a`, `tenant-a`
and `client-a`. OpenAgent's own App, CLI and server are its standalone components.
Provider/protocol adapters are reusable capabilities, not embedding-product adapters.

OpenAgent may support a documented platform or provider difference through a
general contract. It must not infer an embedding application's business rules,
data model, authentication scheme or infrastructure from its name.

## Generic fixes, even when a consumer reports the bug

Reproduce the failing behavior with synthetic identities and a generic public
contract. Implement agent features and fixes in the owning OpenAgent component,
including tools, channels, runners, sessions, memory and capabilities. The fix
must work for any conforming host with the same declared capabilities, rather
than special-casing the reporting consumer. A single reported use case is enough
to justify a generic fix; it is not a reason to encode that use case as policy.

If the architecture cannot express the behavior generically, correct the public
extension point and upstream implementation. Do not add a consumer-local patch,
monkey patch, source overlay, embedded fork or copied runner/tool/channel to work
around it. Consumers adopt exact versioned packages and inherit the capability.

## What remains outside

The embedding application owns its business vocabulary, data, UI branding,
identity verification, domain authorization, credentials and infrastructure.
It supplies these through public interfaces, explicit settings and registered
capability sources. Domain adapters belong with that application. OpenAgent
implements the reusable mechanism, not the application's domain policy.

| Change | Owner |
| --- | --- |
| Runtime/kernel, generic agent behavior, module contracts, shared persistence and SDK | `openagent-core` |
| Generic tool behavior, code-execution backend, browser/computer sidecars, shared connector adapter/runner | `openagent-tools` |
| OpenAgent App/CLI/server composition, identity, channel bridges, client transport and distribution | `openagent` |
| Embedding application's data, branding, permissions and infrastructure adapter | Outside OpenAgent, using public contracts |

Review a change by its owning behavior, not by the repository where the failure
was noticed. New channel behavior must reach every host through the shared bridge
contract; a local UI workaround cannot become a second implementation of it.

## Review and existing debt

Review dependencies and imports as well as names: a generic filename does not
make a consumer-specific implementation reusable. Check that tests exercise the
public contract with neutral fixtures, independent instances and relevant
isolation/cancellation behavior. Qualify upstream and consumer adoption separately.

Existing consumer-specific code or examples are architectural debt, not an
exception to this rule. This documentation change does not certify the entire
source tree as compliant and does not rewrite immutable release receipts or Git
history. Preserve evidence provenance and remove active consumer coupling in
the appropriate upstream change; do not copy private evidence into public docs.
