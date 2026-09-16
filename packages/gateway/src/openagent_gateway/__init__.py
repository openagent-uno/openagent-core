"""Optional HTTP transport. Authentication and identity belong to the host."""
from __future__ import annotations
from dataclasses import asdict
from typing import Any
from aiohttp import web
from openagent_core import RunRequest, IdempotencyConflict

_REQUEST_FIELDS={'run_id','session_id','idempotency_key','input','deadline_seconds','model_ref','attachments','steer_run_id'}


def _id(value):
    if not isinstance(value,str) or not value or len(value)>300 or any(c in value for c in '/\\?#\x00'):
        raise ValueError('Invalid identifier')
    return value


def create_app(runtime, resolve_context, *, resolve_attachments=None, manage_lifecycle=True, prefix='/api/v1/runs'):
    """Build routes without starting a listener or creating identities.

    `resolve_context(request, action, run_id, session_id)` must return an async
    context manager containing a verified ExecutionContext. It can keep temporary
    host credentials scoped around the operation. Only submit has a session_id
    from the request; other operations resolve their resource through the host.
    """
    @web.middleware
    async def boundary(request,handler):
        try:
            return await handler(request)
        except IdempotencyConflict:
            return web.json_response({'error':'idempotency_conflict'},status=409)
        except PermissionError:
            return web.json_response({'error':'not_authorized'},status=403)
        except LookupError:
            return web.json_response({'error':'not_found'},status=404)
        except (ValueError,TypeError):
            return web.json_response({'error':'invalid_request'},status=400)

    async def submit(request):
        body=await request.json()
        if not isinstance(body,dict) or set(body)-_REQUEST_FIELDS:
            raise ValueError('Unexpected request fields')
        run_id,session_id=_id(body.get('run_id')),_id(body.get('session_id'))
        _id(body.get('idempotency_key'))
        if not isinstance(body.get('input'),str): raise ValueError('Invalid input')
        # Reference validation/resolution belongs to the host, not a filesystem
        # path or URL supplied directly by an arbitrary wire client.
        if body.get('attachments') and resolve_attachments is None:
            raise ValueError('The host must expose a verified attachment adapter')
        async with resolve_context(request,'run.submit',run_id,session_id) as context:
            if context.session_id!=session_id: raise PermissionError('Session mismatch')
            attachments=tuple(await resolve_attachments(body['attachments'],context)) if body.get('attachments') else ()
            record=await runtime.submit(RunRequest(run_id,session_id,body['idempotency_key'],body['input'],
                deadline_seconds=body.get('deadline_seconds'),model_ref=body.get('model_ref'),attachments=attachments,
                steer_run_id=_id(body['steer_run_id']) if body.get('steer_run_id') is not None else None),context)
            return web.json_response(asdict(record),status=202,headers={'Location':prefix+'/'+run_id})

    async def read(request):
        run_id=_id(request.match_info['run_id'])
        async with resolve_context(request,'run.read',run_id,None) as context:
            return web.json_response(asdict(await runtime.get_run(run_id,context)))

    async def events(request):
        run_id=_id(request.match_info['run_id'])
        if set(request.query)-{'after'}: raise ValueError('Unknown event query')
        after=int(request.query.get('after','0'))
        if after<0: raise ValueError('Invalid event cursor')
        async with resolve_context(request,'run.replay',run_id,None) as context:
            rows=await runtime.events(run_id,after,context)
            page=rows[:1000]
            return web.json_response({'events':[asdict(row) for row in page],
                'cursor':page[-1].cursor if page else after,'has_more':len(rows)>len(page)})

    async def cancel(request):
        run_id=_id(request.match_info['run_id'])
        async with resolve_context(request,'run.cancel',run_id,None) as context:
            return web.json_response(asdict(await runtime.cancel(run_id,context,reserve=True)))

    async def children(request):
        run_id=_id(request.match_info['run_id'])
        async with resolve_context(request,'run.read',run_id,None) as context:
            return web.json_response({'runs':[asdict(row) for row in await runtime.children(run_id,context)]})

    app=web.Application(middlewares=[boundary],client_max_size=1<<20)
    app.add_routes([web.post(prefix,submit),web.get(prefix+'/{run_id}',read),
        web.get(prefix+'/{run_id}/events',events),web.post(prefix+'/{run_id}/cancel',cancel),
        web.get(prefix+'/{run_id}/children',children)])
    if manage_lifecycle:
        async def lifecycle(app):
            await runtime.start()
            try: yield
            finally: await runtime.close()
        app.cleanup_ctx.append(lifecycle)
    return app

__all__=['create_app']
