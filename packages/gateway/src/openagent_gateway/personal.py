"""Personal catalogs resolved by a host without opening an agent conversation."""
from aiohttp import web
from openagent_core.contracts import ExecutionContext


def add_personal_routes(app, runtime, resolve_personal_context, prefix):
    def service(key):
        bound = runtime.service(key)
        if bound is None:
            raise web.HTTPNotFound()
        return bound

    async def catalog(request):
        resource = request.match_info["catalog"]
        fields = {"q", "cursor", "limit", "ref" if resource == "sessions" else "path"}
        if set(request.query) - fields or any(len(request.query.getall(key)) != 1 for key in request.query):
            raise ValueError("Unexpected personal catalog query")
        limit = int(request.query.get("limit", 50))
        if not 1 <= limit <= 100:
            raise ValueError("Invalid personal catalog page size")
        query, cursor = request.query.get("q"), request.query.get("cursor")
        ref = request.query.get("ref" if resource == "sessions" else "path")
        if ref is not None and (query is not None or (cursor is not None and resource == "memories")):
            raise ValueError("Invalid detail query")
        async with resolve_personal_context(request, resource) as context:
            if (not isinstance(context, ExecutionContext) or context.author != context.initiator
                    or context.authority != context.initiator or context.audience != (context.initiator,)
                    or context.deferred):
                raise PermissionError("The personal catalog requires a verified individual")
            if resource == "sessions":
                sessions = service("sessions.service")
                if ref is not None:
                    result = await sessions.detail(context, ref, limit=limit, cursor=cursor)
                else:
                    result = await sessions.browse(context, query=query, limit=limit, cursor=cursor)
            else:
                vault = service("vault.service")
                documents = getattr(vault, "store", None)
                if documents is None:
                    raise web.HTTPNotFound()
                if ref is not None:
                    result = await documents.read(context, ref)
                elif query is None:
                    result = await documents.list_notes(context, after=cursor, limit=limit)
                else:
                    result = await documents.search_page(context, query=query, after=cursor, limit=limit)
            return web.json_response(result, headers={"Cache-Control": "private, no-store"})

    app.router.add_get(prefix.rstrip("/") + "/{catalog:sessions|memories}", catalog)
