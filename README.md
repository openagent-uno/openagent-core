# OpenAgent core

OpenAgent is an embeddable Python agent runtime. The minimum package exposes
identity, authorization, capability, prompt, lifecycle, and durable run contracts
without installing the standalone product or computer tools.

The optional engine preserves OpenAgent's agent algorithms and memory vault.
Products supply authentication, system instructions, resources, tools, and policy.
GlassPalace builds its own worker and sandbox from pinned core packages; the
standalone app/CLI/server are maintained in the `openagent` product monorepo.

The `1.1.0b3` kernel and SQLite adapter are the current modular release
candidate; optional module wheels remain compatible at `1.1.0b1`. See the
[documentation](docs/README.md), [module contract](docs/modules-v1.1.md), and
[acceptance ledger](docs/migration/implementation.md) for verified behavior and
the boundaries that still require product-specific qualification.

Public architecture and product installation guides are published at
[openagent.uno](https://openagent.uno/guide/architecture). Download the exact
Core wheel set and manifest from
[`v1.1.0-beta.3`](https://github.com/openagent-uno/openagent-core/releases/tag/v1.1.0-beta.3).

```sh
pip install --find-links /path/to/verified-wheelhouse \
  openagent-core==1.1.0b3 openagent-storage-sqlite==1.1.0b3
# Hosts that need the full algorithm select the relevant extras:
pip install --find-links /path/to/verified-wheelhouse \
  'openagent-core[engine,providers,modules,retrieval]==1.1.0b3'
```

Packages shown above are independent distribution names. Hosts pin exact wheel
versions and build their own worker images; they never need a standalone
OpenAgent server release or a source checkout at runtime.
