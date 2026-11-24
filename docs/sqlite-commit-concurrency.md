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

## Scheduler capacity and recovery

Each scheduler admits up to three unattended firings at once by default.
Set `OPENAGENT_BACKGROUND_RUN_CONCURRENCY` to change the process-local
limit. Task and workflow timers, queued requests and event deliveries share
these slots. The event dispatcher also applies its own concurrency limit.
Interactive turns do not take a scheduler slot. Pending work remains in
SQLite when the scheduler is full. The scheduler reports saturation through
the rate-limited `scheduler.background_pressure` event.

A queued task or workflow request carries one stable occurrence identity.
The claim transaction reserves its run row and links the request. A separate
conditional write starts execution. Restart can therefore release a reservation
that never began and dispatch the same occurrence again. A typed pre-admission
delay can also return that occurrence to its queue. An interrupted dispatch
whose effects are unknown is recorded as interrupted. Automatic retries after
dispatch require the existing evidence and retry policy. Historical claimed
requests with no proof of non-admission require inspection.

Task timers reserve their occurrence in the same transaction that advances
the schedule. Workflow timers advance the selected node's schedule and enqueue its exact
occurrence together. The stored schedule and entry node accompany the request
through restart and authorization at dispatch. An idle poll uses a read first
and checks eligibility again inside the write transaction. Event age sweeps
respect unexpired worker leases.

Run projections use the configured SQLite busy timeout for their terminal
write. Once committed, a later conflicting callback preserves that outcome.
A storage failure that exhausts the timeout surfaces as a finalization error.
Retention removes terminal history only. Operators can use the run identity,
the durable session journal and the dispatch error to investigate failures
whose effects cannot be determined automatically.

## Conversation compaction

Compaction reads and writes SQLite off the event loop. It compares the
source digest inside its write transaction before replacing history, including
when a host uses the plain SQLite session table. An intervening update leaves
the newer conversation intact. A preempting interactive turn can cancel a
compactor waiting for the writer. The session lock is held until the compactor
has released its SQLite handle.
