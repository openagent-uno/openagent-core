"""Measure authenticated run admission, serialization and recovery of idle resources on PostgreSQL.

Use a disposable database and set OPENAGENT_BENCH_POSTGRES_DSN. Each invocation
creates a unique schema and retains it for inspection. Run scenarios in separate
processes to measure independent RSS. Supply a UTF-8 PostgreSQL database.
From the repository root, with the runtime, PostgreSQL adapter and psutil installed:

    python benchmarks/session_load.py distinct --runs 1024 --concurrency 32
    python benchmarks/session_load.py hot --runs 160 --concurrency 32

Two worker objects run in the same Python process with independent stores. Half
of the clients wait on another worker when two workers are configured. The
executor only sleeps. --submit-policy-ms models latency of an external
submission authorizer. Wall latency includes admission, owner checks, execution
and confirmation of the terminal receipt. RSS excludes the database process.
"""

import argparse
import asyncio
import gc
import json
import math
import os
import platform
import time
from uuid import uuid4

import psutil
from openagent_core import (
    ExecutionContext,
    PrincipalRef,
    RunRequest,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
)
from openagent_storage_postgres import PostgresRuntimeStore


class Policy:
    def __init__(self, submit_latency):
        self.submit_latency = submit_latency

    async def authorize(self, context, action, resource, *, audience=()):
        if action == "run.submit" and self.submit_latency:
            await asyncio.sleep(self.submit_latency)
        return resource.tenant_id == context.tenant_id and all(p == context.initiator for p in audience)


class Executor:
    def __init__(self, hold):
        self.hold = hold
        self.active = set()
        self.peak = 0

    async def execute(self, request, context, runtime):
        if request.session_id in self.active:
            raise AssertionError("Concurrent execution in one session")
        self.active.add(request.session_id)
        self.peak = max(self.peak, len(self.active))
        try:
            await asyncio.sleep(self.hold)
            return request.run_id
        finally:
            self.active.remove(request.session_id)


def percentiles(samples):
    ordered = sorted(samples)
    return {
        f"p{p}": round(1000 * ordered[math.ceil(p * len(samples) / 100) - 1], 2) for p in (50, 95, 99, 100)
    }


async def run(args):
    dsn = os.environ["OPENAGENT_BENCH_POSTGRES_DSN"]
    schema = "bench_" + uuid4().hex
    executor = Executor(args.hold_ms / 1000)
    policy = Policy(args.submit_policy_ms / 1000)
    process = psutil.Process()
    workers = []
    for _ in range(args.workers):
        store = PostgresRuntimeStore(dsn, agent_id="bench", schema=schema, pool_size=args.pool)
        workers.append(
            Runtime(
                RuntimeSettings("bench", max_active_runs=args.concurrency),
                RuntimeServices(store, executor, policy),
            )
        )

    def counters():
        stores = [w.services.store for w in workers]
        pools = [s._pool.get_stats() for s in stores if s._pool is not None]
        return {
            "rss_mib": round(process.memory_info().rss / 2**20, 2),
            "admissions_in_progress": sum(getattr(w, "_admissions", 0) for w in workers),
            "reserved_slots": sum(len(getattr(w, "_accepting", ())) for w in workers),
            "run_tasks": sum(len(w._tasks) for w in workers),
            "session_locks": sum(len(w._sessions) for w in workers),
            "executing_tasks": sum(len(s._executing) for s in stores),
            "notification_waiters": sum(sum(map(len, s._notices.waiters.values())) for s in stores),
            "pool_connections": sum(p["pool_size"] for p in pools),
            "pool_in_use": sum(p["pool_size"] - p["pool_available"] for p in pools),
            "pool_waiters": sum(p["requests_waiting"] for p in pools),
        }

    def pool_stats():
        rows = [w.services.store._pool.get_stats() for w in workers]
        return {
            k: sum(r.get(k, 0) for r in rows)
            for k in ("requests_num", "requests_queued", "requests_wait_ms", "requests_errors")
        }

    async def turn(index, warmup=False):
        session = f"warmup-{index}" if warmup else "hot" if args.scenario == "hot" else f"session-{index}"
        principal = PrincipalRef("bench", "synthetic", session)
        ctx = ExecutionContext(principal, principal, principal, session, "bench", (principal,))
        ident = uuid4().hex
        request = RunRequest(ident, session, ident, "noop")
        start = time.perf_counter()
        record = await workers[index % len(workers)].submit(request, ctx)
        accepted = time.perf_counter()
        observer = workers[(index + 1) % len(workers)] if index % 2 else workers[index % len(workers)]
        record = await observer.wait(record.run_id, ctx)
        if record.status != "success" or record.output != ident:
            raise RuntimeError(f"Unexpected terminal receipt: {record.status}")
        return accepted - start, time.perf_counter() - start

    sampler = None
    try:
        await asyncio.gather(*(w.start() for w in workers))
        await asyncio.gather(*(turn(n, True) for n in range(args.workers * 4)))
        await asyncio.sleep(0.15)
        gc.collect()
        baseline = counters()
        before = pool_stats()
        executor.peak = 0
        peaks = baseline.copy()

        async def sample():
            while True:
                for k, v in counters().items():
                    peaks[k] = max(peaks[k], v)
                await asyncio.sleep(0.025)

        sampler = asyncio.create_task(sample())
        index = iter(range(args.runs))
        timings = []

        async def client():
            for i in index:
                timings.append(await turn(i))

        started = time.perf_counter()
        await asyncio.gather(*(client() for _ in range(args.concurrency)))
        elapsed = time.perf_counter() - started
        after = pool_stats()
        sampler.cancel()
        await asyncio.gather(sampler, return_exceptions=True)
        await asyncio.sleep(0.3)
        gc.collect()
        idle = counters()
        if any(
            idle[k]
            for k in (
                "admissions_in_progress",
                "reserved_slots",
                "run_tasks",
                "session_locks",
                "executing_tasks",
                "notification_waiters",
                "pool_waiters",
            )
        ):
            raise AssertionError(f"Runtime resources did not drain: {idle}")
        result = {
            "schema": schema,
            "python": platform.python_version(),
            "platform": platform.system(),
            "config": vars(args),
            "runs_verified": len(timings),
            "seconds": round(elapsed, 3),
            "runs_per_second": round(args.runs / elapsed, 2),
            "admission_ms": percentiles([t[0] for t in timings]),
            "submit_to_terminal_ms": percentiles([t[1] for t in timings]),
            "peak_parallel_execution": executor.peak,
            "baseline": baseline,
            "peaks_sampled_at_25ms": peaks,
            "idle": idle,
            "pool_delta": {k: after[k] - before[k] for k in before},
        }
    finally:
        if sampler:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
        await asyncio.gather(*(w.close() for w in workers))
    gc.collect()
    result["after_close"] = counters()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("distinct", "hot"))
    parser.add_argument("--runs", type=int, default=1024)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--pool", type=int, default=8)
    parser.add_argument("--hold-ms", type=float, default=8)
    parser.add_argument("--submit-policy-ms", type=float, default=0)
    args = parser.parse_args()
    if (
        min(args.runs, args.concurrency, args.workers) < 1
        or args.pool < 2
        or any(not math.isfinite(ms) or ms < 0 for ms in (args.hold_ms, args.submit_policy_ms))
    ):
        parser.error("Expected positive runs, concurrency and workers, pool >= 2 and nonnegative delays")
    asyncio.run(run(args))
