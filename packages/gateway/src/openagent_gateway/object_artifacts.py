"""Deliver already-published bytes through the current authenticated session."""
from urllib.parse import quote
from aiohttp import web


def add_artifact_route(app, runtime, resolve_context):
    async def download(request):
        ident = request.match_info["artifact_id"]
        publisher = runtime.service("attachments.publisher")
        if not callable(getattr(publisher, "read_stream", None)):
            raise web.HTTPNotFound()
        async with resolve_context(request, ident) as context:
            ref = await publisher.resolve(context, ident)
            response = web.StreamResponse(headers={
                "Content-Type": ref["mime_type"],
                "Content-Length": str(ref["size_bytes"]),
                "Cache-Control": "private, no-store",
                "X-Content-Type-Options": "nosniff",
                "Content-Disposition": "attachment; filename*=UTF-8''" + quote(ref["filename"], safe=""),
            })
            await response.prepare(request)
            try:
                async for data in publisher.read_stream(context, ident):
                    await response.write(data)
                await response.write_eof()
            except BaseException:
                # Headers are already sent. A short body signals interrupted
                # delivery without appending error text to the attachment.
                if request.transport is not None:
                    request.transport.close()
                raise
            return response

    app.router.add_get("/api/artifacts/{artifact_id:art_[0-9a-f]{32}}/content", download)
