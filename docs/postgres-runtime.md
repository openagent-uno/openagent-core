# PostgreSQL run and session storage

Install the matching `openagent-storage-postgres` wheel alongside the runtime. The authenticated host resolves principals and audience, provides an authorizer and executor, and injects the store.

```python
from openagent_core import Runtime, RuntimeServices, RuntimeSettings
from openagent_storage_postgres import PostgresRuntimeStore

store = PostgresRuntimeStore(dsn, agent_id=agent_id, schema="openagent_runtime")
runtime = Runtime(settings, RuntimeServices(store, executor, authorizer))
await runtime.start()
```

Use a dedicated database or private schema with a trusted database role and transport. The role must own or be permitted to create the selected schema and its objects. Each running process creates a pool (8 connections maximum by default) and a fresh worker identity. Budget the product of replica count and pool size against the database connection limit. Use a single pool per runtime and close the runtime during normal shutdown. Schema selection belongs to host configuration. Session IDs are unique within an agent namespace. User identity comes from the host's authenticated execution context.

## Ownership and ordering

Admission records an idempotency key, request digest, user message and acceptance event in a single transaction. The original process runs the request. Workers execute turns of one session in admission order and can run separate sessions concurrently. Host authorization is checked before submission, execution, replay and publication. Database checks additionally bind session access to tenant, authority, agent and the admitted participants. Session listings, searches and public transcript pages share these boundaries. Session-scoped message IDs and sequences support stable history; event replay uses a separate increasing cursor. Text search uses an authorized substring scan and its cost grows with the selected user’s history.

Every productive writer checks its worker lease using the database clock. A new worker only interrupts work belonging to an expired worker. Recovery closes incomplete tool intents with unknown effects and records a terminal interruption. Interactive work with uncertain side effects requires a new, explicitly authorized request. Named checkpointed operations may use the durable runner to continue at a distinct segment after checking the stored request, segment, operation, grant and checkpoint. The active-operation limit is 100 per agent.

Recovery, remote cancellation and waiting use bounded polling. Remote cancellation is checked approximately once a second. Queue waiting checks approximately every 100 ms per blocked turn. The default lease is 30 seconds. A long database or event-loop stall can expire the worker. Fencing prevents stale writes to the ledger. The host must give external effects their own idempotency keys or fencing when supported by the destination. Keep startup migrations under a single schema owner.

## Coverage and migration

The adapter supports canonical sessions and transcripts, run admission, cancellation, events, bounded replay, final answers, tool intents, recovery and durable operations. The SQL store in this package owns this ledger. A vault, attachment service, provider state, private data erasure workflow and environment provisioning have separate contracts. An embedding host must bind the services its profile uses.

Qualify against a disposable PostgreSQL instance by setting `OPENAGENT_TEST_POSTGRES_DSN` and running `pytest tests/public/test_postgres_runtime.py tests/public/test_postgres_durable_operations.py`. Each test creates a unique schema. Tests cover two HTTP gateways, duplicate admission, ordering, revocation, cross-principal denial, cross-worker cancellation, process death, and the shared durable-operation suite. The wheel release jobs run these suites on PostgreSQL 17.
