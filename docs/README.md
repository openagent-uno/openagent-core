# OpenAgent core documentation

The embedding API is released as the `1.1.0b2` modular candidate. Product
qualification and production cutover remain owned by each host because the core
does not ship product credentials or create infrastructure.

The public product guide is at [openagent.uno](https://openagent.uno/); this
repository remains the canonical source for Core contracts and evidence.

- [Architecture and ownership](architecture.md)
- [Symmetric module contract and product profiles](modules-v1.1.md)
- [Implementation and acceptance ledger](migration/implementation.md)
- [Uniform capability catalog](tool-catalog.md)
- [Memory visibility and durable indexing](memory-access.md)
- [Authorized vault administration](vault-administration.md)
- [Vault/tool parity evidence](migration/vault-tool-parity.md)
- [Prompt rule inventory](migration/prompts.md)
- [Source and Git provenance](migration/sources.json)
- [Historical documentation](legacy/) describes the pre-extraction standalone server.

Start with the architecture, then check the acceptance ledger before using a
component in a deployment. A successful deterministic test does not qualify an
installer, a physical device, a real provider, or a live data migration.
