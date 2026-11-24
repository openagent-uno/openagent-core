"""Optional HTTP transport. Authentication and identity belong to the host."""
from __future__ import annotations
import asyncio
from dataclasses import asdict
from typing import Any
from aiohttp import web
from openagent_core import RunEventPage, RunRequest, IdempotencyConflict
from openagent_core.contracts import AdmissionCapacityExceeded, RunAdmissionPending, ServingUnavailable, WorkerLeaseExpired
from .compression import json_compression
from .computers import ComputerViewerGateway

_REQUEST_FIELDS={'run_id','session_id','idempotency_key','input','deadline_seconds','model_ref','attachments','steer_run_id','admission_policy'}


def _id(value):
    if not isinstance(value,str) or not value or len(value)>300 or any(c in value for c in '/\\?#\x00'):
        raise ValueError('Invalid identifier')
    return value


def create_app(runtime, resolve_context, *, resolve_attachments=None, manage_lifecycle=True,
               prefix='/api/v1/runs', durable_operations=None, resolve_personal_context=None,
               personal_prefix='/api/v1/me', resolve_artifact_context=None, computer_viewer=None,
               serving_generation=None, worker_health=False):
    """Build routes without starting a listener or creating identities.

    `resolve_context(request, action, run_id, session_id)` must return an async
    context manager containing a verified ExecutionContext. It can keep temporary
    host credentials scoped around the operation. Submit and cancellation reservations may supply a session ID. The host
    must authorize the exact session before reserving or reading a run.
    An optional Core DurableOperationRunner owns explicitly selected operations
    and their logical origin status. Continuation policy is supplied by the host.
    """
    runs = durable_operations or runtime
    @web.middleware
    async def boundary(request,handler):
        try:
            return await handler(request)
        except RunAdmissionPending as error:
            return web.json_response(
                {"error": "run_admission_pending", "retry_after_seconds": error.retry_after_seconds},
                status=503, headers={"Retry-After": str(error.retry_after_seconds), "Cache-Control": "no-store"},
            )
        except WorkerLeaseExpired:
            return web.json_response({"error": "worker_lease_expired"}, status=503,
                                     headers={"Retry-After": "1", "Cache-Control": "no-store"})
        except AdmissionCapacityExceeded:
            return web.json_response({'error': 'capacity_exceeded'}, status=429, headers={'Retry-After': '1'})
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
        if "admission_policy" in body and body["admission_policy"] != "defer-unready":
            raise ValueError("Unsupported admission policy")
        options = {"defer_unready": True} if "admission_policy" in body else {}
        # Reference validation/resolution belongs to the host, not a filesystem
        # path or URL supplied directly by an arbitrary wire client.
        if body.get('attachments') and resolve_attachments is None:
            raise ValueError('The host must expose a verified attachment adapter')
        async with resolve_context(request,'run.submit',run_id,session_id) as context:
            if context.session_id!=session_id: raise PermissionError('Session mismatch')
            attachments=tuple(await resolve_attachments(body['attachments'],context)) if body.get('attachments') else ()
            record=await runs.submit(RunRequest(run_id,session_id,body['idempotency_key'],body['input'],
                deadline_seconds=body.get('deadline_seconds'),model_ref=body.get('model_ref'),attachments=attachments,
                steer_run_id=_id(body['steer_run_id']) if body.get('steer_run_id') is not None else None),context,**options)
            return web.json_response(asdict(record),status=202,headers={'Location':prefix+'/'+run_id})

    async def read(request):
        run_id=_id(request.match_info['run_id'])
        async with resolve_context(request,'run.read',run_id,None) as context:
            return web.json_response(asdict(await runs.get_run(run_id,context)))

    async def events(request):
        run_id=_id(request.match_info['run_id'])
        if set(request.query)-{'after','limit'}: raise ValueError('Unknown event query')
        after=int(request.query.get('after','0'))
        limit=int(request.query.get('limit','1000'))
        RunEventPage.validate(after, limit)
        async with resolve_context(request,'run.replay',run_id,None) as context:
            page=await runs.events_page(run_id,after,context,limit=limit)
            return web.json_response(asdict(page))

    async def final_answer(request):
        run_id=_id(request.match_info['run_id'])
        async with resolve_context(request,'run.replay',run_id,None) as context:
            return web.json_response(asdict(await runs.final_answer(run_id,context)),
                                     headers={'Cache-Control':'no-store'})

    async def cancel(request):
        run_id=_id(request.match_info['run_id'])
        body = await request.json() if request.can_read_body else {}
        if not isinstance(body, dict) or set(body) - {"session_id"}:
            raise ValueError("Invalid cancellation reservation")
        session = _id(body["session_id"]) if "session_id" in body else None
        async with resolve_context(request,'run.cancel',run_id,session) as context:
            if session is not None and context.session_id != session:
                raise PermissionError("Session mismatch")
            record = await runs.cancel(run_id, context, reserve=True)
            return web.json_response(asdict(record), headers={"Cache-Control": "no-store"})

    async def cancel_unadmitted(request):
        run_id = _id(request.match_info["run_id"])
        body = await request.json()
        if not isinstance(body, dict) or set(body) != {"session_id"}:
            raise ValueError("A session is required to fence future admission")
        session = _id(body["session_id"])
        async with resolve_context(request, "run.cancel", run_id, session) as context:
            if context.session_id != session:
                raise PermissionError("Session mismatch")
            await runtime.cancel_unadmitted(run_id, context)
            record = await runs.get_run(run_id, context)
            return web.json_response(asdict(record), headers={"Cache-Control": "no-store"})

    async def children(request):
        run_id=_id(request.match_info['run_id'])
        async with resolve_context(request,'run.read',run_id,None) as context:
            return web.json_response({'runs':[asdict(row) for row in await runtime.children(run_id,context)]})

    app=web.Application(middlewares=[json_compression,boundary],client_max_size=1<<20)
    app.add_routes([web.post(prefix,submit),web.get(prefix+'/{run_id}',read),
        web.get(prefix+'/{run_id}/events',events),web.get(prefix+'/{run_id}/final-answer',final_answer),
        web.post(prefix+'/{run_id}/cancel',cancel),
        web.post(prefix+'/{run_id}/cancel-unadmitted',cancel_unadmitted),
        web.get(prefix+'/{run_id}/children',children)])
    if resolve_artifact_context is not None:
        from .object_artifacts import add_artifact_route
        add_artifact_route(app, runtime, resolve_artifact_context)
    if resolve_personal_context is not None:
        from .personal import add_personal_routes
        add_personal_routes(app, runtime, resolve_personal_context, personal_prefix)
    if manage_lifecycle:
        async def lifecycle(app):
            await runtime.start()
            try: yield
            finally: await runtime.close()
        app.cleanup_ctx.append(lifecycle)
    if serving_generation is not None:
        if serving_generation.store is not runtime.services.store:
            raise ValueError('The serving fence must use the active runtime store')
        @web.middleware
        async def serving_fence(request, handler):
            if worker_health and request.match_info.handler is health:
                return await handler(request)
            try:
                async with serving_generation.serving():
                    return await handler(request)
            except ServingUnavailable:
                return web.json_response({'error': 'serving_generation_unavailable'}, status=503,
                                         headers={'Retry-After': '1', 'Cache-Control': 'no-store'})
        app.middlewares.insert(0, serving_fence)
    if worker_health:
        probe = getattr(runtime.services.store, "worker_lease_ready", None)
        if not callable(probe):
            raise ValueError("Worker health requires an authoritative worker lease probe")

        async def health(request):
            live = request.path == "/health/live"
            state = "unknown"
            details = {}
            try:
                async with asyncio.timeout(2):
                    state = "ready" if await probe() else "expired"
                    if state == "ready" and not live and serving_generation is not None:
                        async with serving_generation.serving():
                            pass
            except ServingUnavailable:
                if state == "ready":
                    details["serving_generation"] = "unavailable"
            except Exception:
                # A failed observation can drain traffic, while only confirmed
                # loss of this worker identity should trigger replacement.
                if state == "ready":
                    details["serving_generation"] = "unknown"
            available = not details and (state == "ready" or (live and state == "unknown"))
            return web.json_response({"worker_lease": state, **details}, status=200 if available else 503,
                                     headers={"Cache-Control": "no-store"})

        app.router.add_get("/health/ready", health)
        app.router.add_get("/health/live", health)
    if computer_viewer is not None:
        computer_viewer.install(app, runtime, resolve_context)
    return app

__all__=['ComputerViewerGateway','create_app','json_compression']
