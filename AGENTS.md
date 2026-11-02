# openagent-core

Read `README.md`, `docs/README.md` and the workspace context
`../openagent/docs/PROJECT.md`. This repository owns engine, public runtime contracts, sessions, storage and reusable modules.
Agent features/fixes belong in the owning OpenAgent component, including when
its architecture needs correction. Publish public extension contracts and
versioned capabilities for consumers; never require product-local patches,
overrides, embedded forks or copied runners/tools/channels.
Update the owning documentation and qualification evidence with the change.
Preserve concurrent edits and independent dependency versions.

## Agnostic software — mandatory

Read `docs/AGNOSTICISM.md`, `docs/COMPONENTS.md` and `docs/USAGE.md`.
OpenAgent must contain no references to embedding projects/external consuming
clients and no consumer-specific feature, fix, default, prompt or identity branch.
Use neutral fixtures and public generic contracts. Correct architecture upstream
when needed; consumers inherit versioned capabilities without local patches.
Domain data, branding, authorization and deployment adapters stay outside OpenAgent.
This applies to every OpenAgent repository and its maintained docs/examples/tests.
