"""Deterministic Core storage profile; no network or host credentials.

Run with the runtime source on PYTHONPATH. Output is timing evidence, not an
end-to-end claim. Profiling uses the same seeded database and exact requests.
"""
from __future__ import annotations
import asyncio
import json
import statistics
import tempfile
import time
from pathlib import Path
from openagent_core.engine import MemoryDB

async def main():
    with tempfile.TemporaryDirectory(prefix='core-loading-') as temporary:
        db = MemoryDB(str(Path(temporary) / 'state.sqlite'))
        started = time.perf_counter()
        await db.connect()
        connect_ms = (time.perf_counter() - started) * 1000
        conn = await db._ensure_connected()
        stamp = time.time()
        for index in range(12):
            pid = await db.upsert_provider(name=f'provider-{index}', framework='api-based', metadata={'vendor': 'openai', 'access': 'api'})
            for model in range(8):
                await db.upsert_model(provider_id=pid, model=f'model-{model}')
        await conn.executemany('INSERT INTO usage_log (id,timestamp,model,input_tokens,output_tokens,cost,session_id,year_month) VALUES (?,?,?,?,?,?,?,?)',
            [(str(i), stamp - i * 20, f'provider-{i % 12}:model-{i % 8}', i % 100 + 1, i % 20 + 1, (i % 10) / 1000, f'session-{i % 30}', '2026-11') for i in range(100000)])
        await conn.commit()
        timings = {}
        reads = {
            'catalog': lambda: db.materialise_providers_config(),
            'models': lambda: db.list_models_enriched(),
            'daily_usage': lambda: db.get_daily_usage(30),
            'budget_scope_batch': lambda: asyncio.gather(*[db.get_scope_spend(scope_kind='provider', scope_value=f'provider-{i % 12}', metric='tokens', since_epoch=stamp - 30 * 86400) for i in range(24)]),
        }
        try:
            for name, read in reads.items():
                samples = []
                for _ in range(3):
                    started = time.perf_counter(); result = await read(); samples.append((time.perf_counter() - started) * 1000)
                timings[name] = {'median_ms': statistics.median(samples), 'samples_ms': samples, 'rows': len(result)}
            print(json.dumps({'benchmark':'core-model-loading', 'connect_ms':connect_ms, 'usage_rows':100000, 'providers':12, 'models':96, 'results':timings}, indent=2))
        finally:
            await db.close()

if __name__ == '__main__': asyncio.run(main())
