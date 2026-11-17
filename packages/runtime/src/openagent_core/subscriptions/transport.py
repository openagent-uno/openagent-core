"""Streaming ASGI transport for an in-process provider; never opens a socket."""
from __future__ import annotations

import asyncio
import contextlib
import httpx


class _Body(httpx.AsyncByteStream):
    def __init__(self, queue, task, request):
        self.queue, self.task, self.request = queue, task, request
        self.read_timeout = request.extensions.get("timeout", {}).get("read")

    async def __aiter__(self):
        try:
            while True:
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout=self.read_timeout)
                except asyncio.TimeoutError as exc:
                    raise httpx.ReadTimeout("Subscription response stalled", request=self.request) from exc
                if item is None:
                    await self.task
                    return
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            await self.aclose()

    async def aclose(self):
        if not self.task.done():
            self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.task


class SubscriptionTransport(httpx.AsyncBaseTransport):
    """Backpressure and cancellation survive the SDK's streamed response."""
    def __init__(self, app):
        self.app = app
        self.tasks = set()

    async def handle_async_request(self, request):
        if request.url.host != "subscription.internal":
            raise ValueError("Subscription transport cannot contact external URLs")
        queue = asyncio.Queue(maxsize=8)
        started = asyncio.get_running_loop().create_future()
        body = await request.aread()
        received = False
        complete = asyncio.Event()
        response_finished = False

        async def receive():
            nonlocal received
            if not received:
                received = True
                return {"type": "http.request", "body": body, "more_body": False}
            await complete.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            nonlocal response_finished
            if message["type"] == "http.response.start":
                started.set_result((message["status"], message.get("headers", [])))
            elif message["type"] == "http.response.body":
                if message.get("body"):
                    await queue.put(message["body"])
                if not message.get("more_body", False):
                    response_finished = True
                    await queue.put(None)
                    complete.set()

        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1", "method": request.method,
            "headers": [(key.lower(), value) for key, value in request.headers.raw], "scheme": "http",
            "path": request.url.path, "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query, "root_path": "",
            "server": ("subscription.internal", 80), "client": ("127.0.0.1", 0),
        }

        async def run():
            try:
                await self.app(scope, receive, send)
                if not started.done():
                    started.set_exception(RuntimeError("Subscription provider returned no response"))
                elif not response_finished:
                    await queue.put(RuntimeError("Subscription provider returned an incomplete response"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not started.done():
                    started.set_exception(exc)
                else:
                    await queue.put(exc)
            finally:
                complete.set()

        task = asyncio.create_task(run())
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        try:
            try:
                status, headers = await asyncio.wait_for(started, timeout=request.extensions.get("timeout", {}).get("read"))
            except asyncio.TimeoutError as exc:
                raise httpx.ReadTimeout("Subscription response headers stalled", request=request) from exc
        except BaseException:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            raise
        return httpx.Response(status, headers=headers, stream=_Body(queue, task, request), request=request)

    async def aclose(self):
        tasks = tuple(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
