"""Resource lifetime shared by direct and routing model adapters."""
from __future__ import annotations
import asyncio
import contextlib
import functools
import inspect

def _provider_call(method):
    """Hold cached resources until every overlapping model call has finished."""
    if inspect.isasyncgenfunction(method):
        @functools.wraps(method)
        async def stream(self, *args, **kwargs):
            async with self._call_scope():
                iterator = method(self, *args, **kwargs)
                try:
                    async for item in iterator:
                        yield item
                finally:
                    await iterator.aclose()
        return stream
    @functools.wraps(method)
    async def complete(self, *args, **kwargs):
        async with self._call_scope():
            return await method(self, *args, **kwargs)
    return complete


class ProviderResources:
    def _initialize_resources(self):
        self._retired_runtimes = []
        self._active_calls = 0
        self._closing = False
        self._idle = asyncio.Event()
        self._idle.set()

    def _retire_runtime(self, runtime) -> None:
        self._retired_runtimes.append(runtime)

    async def _release_retired(self) -> None:
        from openagent_core.models.runtime_db_lifecycle import close_runtime_clients, close_runtime_databases
        if self._active_calls:
            return
        retired, self._retired_runtimes = self._retired_runtimes, []
        seen = set()
        for runtime in retired:
            if id(runtime) not in seen:
                seen.add(id(runtime))
                shutdown = getattr(runtime, "shutdown", None)
                if callable(shutdown):
                    result = shutdown()
                    if inspect.isawaitable(result):
                        await result
                close_runtime_databases(runtime)
                await close_runtime_clients(runtime)

    @contextlib.asynccontextmanager
    async def _call_scope(self):
        if self._closing:
            raise RuntimeError("Model provider is closed")
        self._active_calls += 1
        self._idle.clear()
        try:
            yield
        finally:
            self._active_calls -= 1
            if not self._active_calls:
                try:
                    await self._release_retired()
                finally:
                    if not self._active_calls:
                        self._idle.set()

