<!-- Concrete host labels in this historical record are anonymized; original bytes and hashes are retained in the protected operator audit. -->
# OpenAgent core

OpenAgent is an embeddable Python agent runtime. The minimum package exposes
identity, authorization, capability, prompt, lifecycle, and durable run contracts
without installing the standalone product or computer tools.

The optional engine preserves OpenAgent's agent algorithms and memory vault.
Products supply authentication, system instructions, resources, tools, and policy.
embedding-host builds its own worker and sandbox from pinned core packages; the
standalone app/CLI/server are maintained in the `openagent` product monorepo.

The `1.1.0b35` kernel, SQLite adapter, gateway and SDK add bounded completion
repair when an authenticated send-file call is omitted from the terminal reply.
The same model must author the exact markers; publication and recipient checks
remain in force. Beta29 confines final-answer media to exact markers authored
in the attested final message, preserving input/tool previews only in the full
transcript. Observed terminal background results no longer reopen the model
loop; unobserved reminders request a complete answer to the original task.
Beta28 isolates the audio worker protocol from vendor
asynchronous/shutdown logging. Beta27 adds authoritative completed-run final answers alongside
the unchanged full transcript; see [the contract](docs/final-answer.md).
Beta24 added canonical original-input history and explicit
host-authorized output attachment delivery to the beta23 previews/delegation
contract. Deterministic source tests are recorded separately from live
provider/product qualification, which remains owned by the embedding host. The module compatibility and full-profile packages are
`1.1.0b3`. See the
[documentation](docs/README.md), [module contract](docs/modules-v1.1.md), and
[acceptance ledger](docs/migration/implementation.md) for verified behavior and
the boundaries that still require product-specific qualification.

Public architecture and product installation guides are published at
[openagent.uno](https://openagent.uno/guide/architecture). Download the exact
Core wheel set and manifest from
[`v1.1.0-beta.35`](https://github.com/openagent-uno/openagent-core/releases/tag/v1.1.0-beta.35).

```sh
pip install --find-links /path/to/verified-wheelhouse \
  openagent-core==1.1.0b35 openagent-storage-sqlite==1.1.0b35
# Hosts that need the full algorithm select the relevant extras:
pip install --find-links /path/to/verified-wheelhouse \
  'openagent-core[engine,providers,modules,retrieval]==1.1.0b35'
```

Packages shown above are independent distribution names. Hosts pin exact wheel
versions and build their own worker images; they never need a standalone
OpenAgent server release or a source checkout at runtime.
