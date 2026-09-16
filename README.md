# OpenAgent core

OpenAgent is an embeddable Python agent runtime. The minimum package exposes
identity, authorization, capability, prompt, lifecycle, and durable run contracts
without installing the standalone product or computer tools.

The optional engine preserves OpenAgent's agent algorithms and memory vault.
Products supply authentication, system instructions, resources, tools, and policy.
GlassPalace builds its own worker and sandbox from pinned core packages; the
standalone app/CLI/server are maintained in the `openagent` product monorepo.

This isolated `1.0.0b1` migration branch is **under implementation**. See the
[documentation](docs/README.md) and [acceptance ledger](docs/migration/implementation.md)
for verified behavior and the remaining release gates.

```sh
pip install openagent-core openagent-storage-sqlite
# Hosts that need the full algorithm select the relevant extras:
pip install 'openagent-core[engine,providers,modules,retrieval]'
```

Packages shown above are distribution names; this prerelease is not yet
published. Local qualification installs built wheels from the migration wheelhouse.
