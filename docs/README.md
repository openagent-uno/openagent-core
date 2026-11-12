# OpenAgent core documentation

## Architecture and use

- [Unreleased deferred-tool child-card provenance](session-history.md#branching-a-conversation-at-a-message)
  ([source qualification](qualification/deferred-child-link-2026-11-12.json)).

- [Beta81: explicit stream stop terminal boundary](releases/v1.1.0-beta.81.md).

- [Beta80: confirmed outgoing conversation history](releases/v1.1.0-beta.80.md).

- [Beta79: one-level delegation and isolated fallback refresh](releases/v1.1.0-beta.79.md).

- [Beta78: destination discovery and session-linked tool images](releases/v1.1.0-beta.78.md).

- [Beta66: find and resend original session attachments](releases/v1.1.0-beta.66.md).
- [Beta65: opt-in native JSON Schema for staged inference](releases/v1.1.0-beta.65.md).
- [Beta64: host-selected speech synthesis deadline](releases/v1.1.0-beta.64.md).
- [Beta63: bounded retries for structured staged completions](releases/v1.1.0-beta.63.md).
- [Beta61: image evidence in staged completions](releases/v1.1.0-beta.61.md).
- [Beta60: desktop interaction for authorized host viewers](releases/v1.1.0-beta.60.md).

- [Beta59: bounded video preview resolution for staged operations](releases/v1.1.0-beta.59.md).

- [Beta58: checkpointed staged operations](releases/v1.1.0-beta.58.md) and the
  [embedding contract](staged-operations.md).

- [Beta57: opt-in named calls through the authorized tool catalog](releases/v1.1.0-beta.57.md).

- [Beta56: optional visual previews of tool videos and speech synthesis](releases/v1.1.0-beta.56.md).

- [Author and discover skills](skills.md).

- [MCP marketplace in agent sessions](mcp-marketplace.md).

- [Agent activation of reviewed MCP integrations](reviewed-mcp-installations.md).

- [Optional current-user notification capability](notifications.md).

- [Beta54: optional video frame previews for image models](releases/v1.1.0-beta.54.md).

- [Beta53: complete isolated engine qualification and current-session recovery](releases/v1.1.0-beta.53.md)
  ([qualification procedure](USAGE.md#reproducible-engine-qualification)).

- [Unreleased compact cross-source BM25 discovery](tool-catalog.md#lexical-retrieval-unreleased-source-candidate)
  and [current-conversation history recovery](session-history.md#resuming-a-conversation-and-recovering-omitted-details).

- [Beta52: lightweight configured OpenAI speech route](audio-host-contract.md).
- [MCP transport recovery and optional per-server read deadlines](mcp-transport-recovery.md)

- [Beta50: bounded optional summaries and preserved provider failures](releases/v1.1.0-beta.50.md) ([qualification](qualification/async-summary-2026-11-06-b50.json)).

- [Core beta44: discovery and optional-import contracts](releases/v1.1.0-beta.44.md)

- [Agnostic architecture: mandatory in every OpenAgent repository](AGNOSTICISM.md)
- [Component responsibilities, organization and communication](COMPONENTS.md)
- [Using the kernel, clients, tools and runners](USAGE.md)

- [Discovery presentation API v1: authorized bounded hints and descriptions, unreleased source](discovery-presentation.md)


- [Local MCP startup recovery and credential-free diagnostics candidate](mcp-startup-diagnostics.md)

- [Beta42 local release preparation: shared chat commands](releases/v1.1.0-beta.42.md)

- [Shared chat commands: one catalogue, deferred cleanup ordering and local review](chat-commands.md)

- [Beta41: same-task MCP partial-entry rollback](releases/v1.1.0-beta.41.md) — full and installed-wheel qualifications recorded.

- [Bounded completion of explicitly requested file delivery](final-answer.md#prepared-files-omitted-by-the-final-answer)

- [Interactive tool budgets and final delivery](interactive-tool-budgets.md)

The published modular family is `1.1.0b54`: Core, SQLite, gateway and Python SDK
pin the same Core wheel. The independent capability host is `1.0.0b3` and
continues to consume tool protocol `1.0.0b2`. Core and Python SDK expose an
optional `capabilities` extra requiring capability-host `1.0.0b3`; the base
embedding package keeps no mandatory MCP dependency.
See [the beta40 SQLite resource lifetime fix](releases/v1.1.0-beta.40.md).
It retains [beta39 connection-generation protections](releases/v1.1.0-beta.39.md)
and [the beta38 integration boundary](releases/v1.1.0-beta.38.md). Publication,
installed-consumer qualification and signed product bundles remain separate.
Beta27 adds an optional authoritative `final_answer` envelope while preserving
the complete transcript. Concurrent upstream canonical-session indexing and
retention fixes are included; host, provider and live-channel acceptance remain
separate from deterministic package qualification.
Beta31 recognizes exactly verified legacy projection domains beside canonical
runtime rows, including normal retained deletion tombstones, without granting
the legacy namespace runtime authority or rewriting storage identity. Foreign
canonical rows and unbacked projections remain denied.
Beta30 restores exact admitted inputs and partial tools to public history after
an interrupted provider write, preserves cancelled turns and attests canonical
child ancestry independently of display origin. Both embedded hosts and the
standalone server use this shared read-only service.
Beta29 prevents full-transcript input/tool preview media from being echoed as
final-answer attachments. Both Engine and StreamSession bind exact authored
final markers to normally authorized canonical refs; live snapshots recheck
only those selected final refs. The complete transcript remains unchanged.
An optional trusted post-publication executor hook acknowledges terminal jobs
already read through tools; unobserved internal reminders request a complete
final answer rather than assuming earlier narration was delivered.
Beta28 fixes the isolated audio worker stdout contract. It keeps vendor asynchronous and interpreter-shutdown
logs off its single-result JSON pipe; see the audio host contract.
The neutral [minimal runtime example](../examples/minimal-runtime.py)
uses the public kernel without engine or standalone services. Product
qualification and production cutover remain owned by each host because the core
does not ship product credentials or create infrastructure.

Automation toolkits expose explicit read-only revision review and exact-digest
approval operations. Historical scheduled tasks, workflows and events remain
paused until an authenticated product host captures that approval; Core never
assigns them an owner during startup or migration.

Core infers image input for recognised Claude model IDs even when an
OpenAI-compatible subscription proxy is registered under a custom provider
name. Explicit `models.metadata.input_modalities` declarations remain
authoritative. Existing rows already persisted as text-only must be reviewed
and updated by their host after verifying that the proxy accepts images.
Discovery preserves `input_modalities`, `capabilities`, and optional
`image_model_id` advertised by OpenAI-compatible `/v1/models` endpoints,
including custom providers with a configured base URL. A base URL ending in
`/v1` resolves to `/v1/models` once. Core reports the provider contract;
the standalone product or embedding host owns persistence and reconciliation.

The public product guide is at [openagent.uno](https://openagent.uno/); this
repository remains the canonical source for Core contracts and evidence.

- [Architecture and ownership](architecture.md)
- [Opt-in instance-owned management transport and source qualification](management-inprocess.md)
- [Symmetric module contract and product profiles](modules-v1.1.md)
- [Implementation and acceptance ledger](migration/implementation.md)
- [Uniform capability catalog](tool-catalog.md)
- [Complete MCP catalogs and live local refresh, unreleased source qualification](mcp-catalog-refresh.md)
- [Attachment admission, previews, canonical history and explicit output delivery](attachments.md)
- [Core b24 canonical attachment history/delivery evidence](evidence/attachments-b24-2026-10-19.md)
- [Core b23 deterministic attachment/delegation evidence](evidence/attachments-b23-2026-10-19.md)
- [Image model and attachment contract](image-generation.md)
- [Audio host and transcribed attachment contract](audio-host-contract.md)
- [Authoritative final answers and the complete stream](final-answer.md)
- [Core b29 final media boundary source evidence](evidence/final-answer-media-b29-2026-10-23.json)
- [Core b29 official public package qualification](evidence/core-b29-public-2026-10-23.json)
- [Core b30 official public package qualification](evidence/core-b30-public-2026-10-23.json)
- [Core b31 official public package and isolated restart qualification](evidence/core-b31-public-2026-10-23.json)
- [Selective quarantined legacy-child adoption](legacy-child-adoption.md)
- [Empty legacy-placeholder namespace adoption](legacy-empty-child-adoption.md)
- [Sessions and exact run controls](../packages/module-sessions/README.md)
- [Storage tenant checks and verified legacy projection domains](storage-tenant-domains.md)
- [Public history after interrupted execution and canonical ancestry](session-history.md)
- [Memory visibility and durable indexing](memory-access.md)
- [Incremental operational maintenance and idle resource bounds](operational-maintenance.md)
- [Derived content privacy contracts and current coverage](content-privacy.md)
- [Authorized vault administration](vault-administration.md)
- [Vault/tool parity evidence](migration/vault-tool-parity.md)
- [Prompt rule inventory](migration/prompts.md)
- [Source and Git provenance](migration/sources.json)
- [Historical documentation](legacy/) describes the pre-extraction standalone server.

Start with the architecture, then check the acceptance ledger before using a
component in a deployment. A successful deterministic test does not qualify an
installer, a physical device, a real provider, or a live data migration.
The run observer now waits for terminal state across worker processes as well
as within one runtime; the Sessions module limits model-facing waits to 30 seconds.

- [Optional MCP Apps host service and memory bounds](mcp-apps.md)

- [Native private state permission checks](private-state-permissions.md)

The beta45 candidate adds the dependency-free native permission API; its
source and wheel workflows distinguish platform contracts from host acceptance.

- [Optional remote capabilities, inventory and media](remote-capabilities.md)
