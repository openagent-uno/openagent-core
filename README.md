# OpenAgent core

OpenAgent is an embeddable Python agent runtime. The minimum package exposes
identity, authorization, capability, prompt, lifecycle, and durable run contracts
without installing the standalone product or computer tools.

The optional engine preserves OpenAgent's agent algorithms and memory vault.
Products supply authentication, system instructions, resources, tools, and policy.
GlassPalace builds its own worker and sandbox from pinned core packages; the
standalone app/CLI/server are maintained in the `openagent` product monorepo.

The `1.1.0b1` package set is the first modular release candidate. See the
[documentation](docs/README.md), [module contract](docs/modules-v1.1.md), and
[acceptance ledger](docs/migration/implementation.md) for verified behavior and
the boundaries that still require product-specific qualification.

```sh
pip install openagent-core openagent-storage-sqlite
# Hosts that need the full algorithm select the relevant extras:
pip install 'openagent-core[engine,providers,modules,retrieval]'
```

Packages shown above are independent distribution names. Hosts pin exact wheel
versions and build their own worker images; they never need a standalone
OpenAgent server release or a source checkout at runtime.
