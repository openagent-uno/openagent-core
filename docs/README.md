# OpenAgent core documentation

The embedding API is prepared as the `1.1.0b29` modular candidate. Its SQLite
adapter, gateway and Python SDK share that version and pin the same Core wheel.
Beta27 adds an optional authoritative `final_answer` envelope while preserving
the complete transcript. Concurrent upstream canonical-session indexing and
retention fixes are included; host, provider and live-channel acceptance remain
separate from deterministic package qualification.
Beta29 prevents full-transcript input/tool preview media from being echoed as
final-answer attachments. Both Engine and StreamSession bind exact authored
final markers to normally authorized canonical refs; live snapshots recheck
only those selected final refs. The complete transcript remains unchanged.
An optional trusted post-publication executor hook acknowledges terminal jobs
already read through tools; unobserved internal reminders request a complete
final answer rather than assuming earlier narration was delivered.
Beta28 fixes the isolated audio worker stdout contract. It keeps vendor asynchronous and interpreter-shutdown
logs off its single-result JSON pipe; see the audio host contract.
The Replio example is `1.1.0b2` and
uses the new Sessions module. Product
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
- [Symmetric module contract and product profiles](modules-v1.1.md)
- [Implementation and acceptance ledger](migration/implementation.md)
- [Uniform capability catalog](tool-catalog.md)
- [Attachment admission, previews, canonical history and explicit output delivery](attachments.md)
- [Core b24 canonical attachment history/delivery evidence](evidence/attachments-b24-2026-10-19.md)
- [Core b23 deterministic attachment/delegation evidence](evidence/attachments-b23-2026-10-19.md)
- [Image model and attachment contract](image-generation.md)
- [Audio host and transcribed attachment contract](audio-host-contract.md)
- [Authoritative final answers and the complete stream](final-answer.md)
- [Core b29 final media boundary source evidence](evidence/final-answer-media-b29-2026-10-23.json)
- [Core b29 official public package qualification](evidence/core-b29-public-2026-10-23.json)
- [Selective quarantined legacy-child adoption](legacy-child-adoption.md)
- [Empty legacy-placeholder namespace adoption](legacy-empty-child-adoption.md)
- [Sessions and exact run controls](../packages/module-sessions/README.md)
- [Memory visibility and durable indexing](memory-access.md)
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
