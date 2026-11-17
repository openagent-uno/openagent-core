"""Embedded workers must not multiply an account's upstream concurrency limit."""
from pathlib import Path
import multiprocessing
import os
import time
import pytest
from openagent_core.subscriptions.admission import StorePacer, StoreSemaphore


def _lease_worker(path, start, active, peak, completed):
    slots = StoreSemaphore(Path(path), 'active', 1)
    start.wait(10)
    if not slots.acquire(timeout=10): return
    try:
        with active.get_lock():
            active.value += 1
            peak.value = max(peak.value, active.value)
        time.sleep(0.05)
        with active.get_lock():
            active.value -= 1
            completed.value += 1
    finally:
        slots.release()


def _crashing_worker(path, ready):
    import os
    slots = StoreSemaphore(Path(path), 'active', 1)
    if slots.acquire(timeout=5):
        ready.set()
        os._exit(0)


def test_account_concurrency_is_shared_across_processes_and_crashed_workers_release_it(tmp_path):
    context = multiprocessing.get_context('spawn')
    active, peak, completed = [context.Value('i', 0) for _ in range(3)]
    start = context.Event(); path = str(tmp_path / 'store.json')
    workers = [context.Process(target=_lease_worker, args=(path,start,active,peak,completed)) for _ in range(4)]
    try:
        for worker in workers: worker.start()
        start.set()
        for worker in workers:
            worker.join(timeout=15); assert worker.exitcode == 0
        assert peak.value == 1 and completed.value == 4
        ready = context.Event()
        crashed = context.Process(target=_crashing_worker, args=(path,ready)); workers.append(crashed)
        crashed.start(); assert ready.wait(10)
        crashed.join(timeout=10); assert crashed.exitcode == 0
        slots = StoreSemaphore(Path(path), 'active', 1)
        assert slots.acquire(timeout=1)
        slots.release()
    finally:
        for worker in workers:
            if worker.is_alive(): worker.terminate()
            worker.join(timeout=5)


def test_queue_is_bounded_and_cannot_release_an_unowned_slot(tmp_path):
    a = StoreSemaphore(tmp_path / 'store', 'queue', 2)
    b = StoreSemaphore(tmp_path / 'store', 'queue', 2)
    assert a.acquire(blocking=False) and a.acquire(blocking=False)
    assert b.acquire(blocking=False) is False
    a.release(); assert b.acquire(blocking=False)
    a.release(); b.release()
    with pytest.raises(ValueError): b.release()
    if os.name != 'nt':
        assert all(p.stat().st_mode & 0o777 == 0o600 for p in tmp_path.glob('*.lock'))


def test_pacing_and_upstream_backoff_are_shared_between_service_instances(tmp_path, monkeypatch):
    from openagent_core.subscriptions import admission
    monkeypatch.setattr(admission.time, 'time', lambda: 100)
    a, b = StorePacer(tmp_path / 'store'), StorePacer(tmp_path / 'store')
    assert a.reserve(2) == 0
    assert b.reserve(2) == 2
    b.cool_down(10)
    assert a.reserve(2) == 10
    if os.name != 'nt':
        assert a.path.stat().st_mode & 0o777 == 0o600
