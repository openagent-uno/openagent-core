# Incremental operational maintenance

`openagent_core.memory.operational.maintenance.maintain_operational_storage`
owns reconciliation, legacy backfill, automation projection and derived search
maintenance. A host supplies its existing `MemoryDB` and a status callback.
The host owns task startup/cancellation and maps the status into its transport
readiness; it does not copy the maintenance algorithm.

After reaching ready, unchanged polls use indexed journal/outbox probes and
index-file metadata. They do not count or scan the complete history or FTS
corpus every half second. New durable changes and changed/deleted index files
resume normal reconciliation on the next poll. A full audit still runs every
60 seconds by default, including when file timestamps have not changed.
Explicit search/status operations retain their full validation. Canonical
authorization and journal freshness checks remain authoritative for reads.

`poll_interval` and `audit_interval` must be positive. Status callbacks may be
synchronous or asynchronous and receive detached search metadata. Storage
errors publish degraded status, suppress private exception details and retry
with bounded backoff. Cancellation drains existing SQLite work before closing
the worker-owned connection. Canonical data and identity are never reset.
The owned writer uses the existing runtime SQLite busy-timeout configuration.
