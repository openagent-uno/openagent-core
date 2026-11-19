"""Keep a blocking protocol iterator and its leases on one cancellable worker."""
from __future__ import annotations

import asyncio
import contextvars
import queue
import threading
import time

_cancel = contextvars.ContextVar('subscription-request-cancel', default=None)


class UpstreamCancelled(RuntimeError):
    pass


def cancel_event():
    return _cancel.get()


def check_cancelled():
    event = cancel_event()
    if event is not None and event.is_set():
        raise UpstreamCancelled('Subscription request cancelled')


def sleep(seconds):
    event = cancel_event()
    if event is None:
        time.sleep(seconds)
    elif event.wait(max(0, seconds)):
        check_cancelled()


def acquire(semaphore, timeout):
    event = cancel_event()
    if event is None:
        return semaphore.acquire(timeout=timeout)
    deadline = time.monotonic() + max(0, timeout)
    while True:
        check_cancelled()
        remaining = deadline - time.monotonic()
        if semaphore.acquire(timeout=min(0.05, max(0, remaining))):
            if event.is_set():
                semaphore.release()
                check_cancelled()
            return True
        if remaining <= 0:
            return False


def close_on_cancel(close):
    """Interrupt a blocking response read without retaining a watcher afterward."""
    event = cancel_event()
    finished = threading.Event()
    if event is not None:
        def watch():
            while not finished.wait(0.05):
                if event.is_set():
                    try:
                        close()
                    except Exception:
                        pass
                    return
        threading.Thread(target=watch, name='subscription-response-cancel', daemon=True).start()
    return finished


async def iterate(iterator, *, heartbeat_s=1.0):
    """Run every next/close on one thread, with bounded buffering and cancellation.

    Moving a suspended generator between thread-pool workers loses ownership of
    thread-local file locks. The producer must close that same generator even
    when a consumer leaves between chunks. No default executor shutdown wait is
    introduced by a blocked external socket.
    """
    event = threading.Event()
    output = queue.Queue(maxsize=8)
    done = object()
    def put(item):
        while not event.is_set():
            try:
                output.put(item, timeout=0.05)
                return True
            except queue.Full:
                pass
        return False
    def produce():
        token = _cancel.set(event)
        try:
            try:
                for item in iterator:
                    check_cancelled()
                    if not put((True, item)):
                        break
            except BaseException as exc:
                put((False, exc))
            finally:
                close = getattr(iterator, 'close', None)
                if close is not None:
                    try:
                        close()
                    except BaseException as exc:
                        put((False, exc))
        finally:
            _cancel.reset(token)
            put(done)
    context = contextvars.copy_context()
    threading.Thread(target=context.run, args=(produce,), name='subscription-stream', daemon=True).start()
    next_heartbeat = time.monotonic() + heartbeat_s if heartbeat_s else None
    try:
        while True:
            try:
                item = output.get_nowait()
            except queue.Empty:
                if next_heartbeat is not None and time.monotonic() >= next_heartbeat:
                    next_heartbeat = time.monotonic() + heartbeat_s
                    yield ': keepalive\n\n'
                await asyncio.sleep(0.01)
                continue
            if item is done:
                return
            success, value = item
            if not success:
                raise value
            yield value
    finally:
        event.set()


async def run(function, *args):
    def result():
        yield function(*args)
    stream = iterate(result(), heartbeat_s=None)
    try:
        return await anext(stream)
    finally:
        await stream.aclose()
