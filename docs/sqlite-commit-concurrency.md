# Shared SQLite journal writes

Engine storage can append session events while another authorized operation
saves state through the same `MemoryDB` connection. A write with `RETURNING`
must finish consuming its result before another connection user can commit.
Separate asynchronous execute/fetch/close steps left a window in which that
commit failed with SQLite's `SQL statements in progress` error.

Session event appends, orphan task-run updates and operational history revision
allocation execute and consume their returned rows within one SQLite worker
operation. This also covers the shared connection of in-memory operational
maintenance. An unrelated state save can
then commit without encountering that pending write cursor. Sequence allocation,
journal privacy dispatch, best-effort failure handling, orphan status distinctions
and recovery identifiers retain their existing contracts. This does not introduce
a general transaction boundary across separate public method calls.

The public regression uses real SQLite and concurrent session-journal/state
writes, checking successful outcomes, durable data and unique journal sequences.
It does not certify any host deployment or messaging provider.
