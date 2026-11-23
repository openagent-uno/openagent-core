"""Authenticated HTTP SDK. The host authenticates every request's headers.

No principal, delegation, device lease, or executor is accepted from caller JSON.
A network failure after submission is uncertain; reconcile the same run ID.
"""
from __future__ import annotations
import asyncio
from dataclasses import asdict
import inspect
import json
from typing import Callable, Mapping, Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler
from openagent_core import RunRequest, RunRecord, RunEvent, RunEventPage, RunFinalAnswer


class RunHTTPError(RuntimeError):
    def __init__(self,status: int):
        self.status=status
        super().__init__(f'Run service returned HTTP {status}')


class AcceptanceUncertain(ConnectionError):
    def __init__(self,run_id: str):
        self.run_id=run_id
        super().__init__('Acceptance is uncertain; reconcile this same run ID')


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None


def _record(value):
    if 'origin_run_id' in value:
        from openagent_core.durable_operations import DurableRunRecord
        return DurableRunRecord(**value)
    return RunRecord(**value)


class _HTTPClient:
    def __init__(self,base_url: str,headers: Callable[[], Any],*,path='/api/v1/runs',timeout=10):
        parsed=urlsplit(base_url)
        if parsed.scheme not in {'http','https'} or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError('An explicit HTTP service origin is required')
        self._base=base_url.rstrip('/')+path.rstrip('/')
        self._headers=headers
        self.timeout=timeout

    async def _request(self,method,path='',body=None):
        headers=self._headers()
        if inspect.isawaitable(headers): headers=await headers
        headers={**dict(headers),'Accept':'application/json'}
        data=json.dumps(body,allow_nan=False,separators=(',',':')).encode() if body is not None else None
        if data is not None: headers['Content-Type']='application/json'
        request=Request(self._base+path,data=data,headers=headers,method=method)
        def fetch():
            try:
                with build_opener(_NoRedirect).open(request,timeout=self.timeout) as response:
                    raw=response.read(16*1024*1024+1)
                    if len(raw)>16*1024*1024: raise ValueError('Run response exceeds client limit')
                    return json.loads(raw)
            except HTTPError as error:
                error.close()
                raise RunHTTPError(error.code) from None
        return await asyncio.to_thread(fetch)


class PersonalClient(_HTTPClient):
    """Read the signed-in caller's sessions and document vault."""
    def __init__(self, base_url, headers, *, path="/api/v1/me", timeout=10):
        super().__init__(base_url, headers, path=path, timeout=timeout)

    async def _catalog(self, resource, **fields):
        query = urlencode({key: value for key, value in fields.items() if value is not None})
        return await self._request("GET", "/" + resource + ("?" + query if query else ""))

    async def sessions(self, *, query=None, cursor=None, limit=50):
        return await self._catalog("sessions", q=query, cursor=cursor, limit=limit)

    async def session(self, session_ref, *, cursor=None, limit=50):
        return await self._catalog("sessions", ref=session_ref, cursor=cursor, limit=limit)

    async def memories(self, *, query=None, cursor=None, limit=50, space=None):
        return await self._catalog("memories", q=query, cursor=cursor, limit=limit, space=space)

    async def memory(self, path, *, space=None):
        return await self._catalog("memories", path=path, space=space)


class RunClient(_HTTPClient):
    async def submit(self,request: RunRequest) -> RunRecord:
        try:
            value=await self._request('POST',body=asdict(request))
        except RunHTTPError as exc:
            if exc.status >= 500:
                raise AcceptanceUncertain(request.run_id) from exc
            raise
        except (URLError,TimeoutError,ConnectionError,OSError,json.JSONDecodeError) as exc:
            raise AcceptanceUncertain(request.run_id) from exc
        return _record(value)

    async def get(self,run_id: str) -> RunRecord:
        return _record(await self._request('GET','/'+quote(run_id,safe='')))

    async def events(self,run_id: str,after: int=0) -> tuple[RunEvent,...]:
        return (await self.events_page(run_id,after)).events

    async def events_page(self,run_id: str,after: int=0,*,limit: int=1000) -> RunEventPage:
        RunEventPage.validate(after, limit)
        path='/'+quote(run_id,safe='')+'/events?after='+str(after)
        if limit != 1000:
            path+='&limit='+str(limit)
        result=await self._request('GET',path)
        return RunEventPage(tuple(RunEvent(**event) for event in result['events']),
                            result['cursor'],result['has_more'])

    async def final_answer(self,run_id: str) -> RunFinalAnswer:
        return RunFinalAnswer(**await self._request('GET','/'+quote(run_id,safe='')+'/final-answer'))

    async def cancel(self,run_id: str) -> RunRecord:
        return _record(await self._request('POST','/'+quote(run_id,safe='')+'/cancel'))

    async def children(self,run_id: str) -> tuple[RunRecord,...]:
        result=await self._request('GET','/'+quote(run_id,safe='')+'/children')
        return tuple(_record(row) for row in result['runs'])

    async def wait(self,run_id: str,*,interval: float=.5) -> RunRecord:
        if interval<=0: raise ValueError('Polling interval must be positive')
        while True:
            result=await self.get(run_id)
            if result.terminal: return result
            await asyncio.sleep(interval)


__all__=['PersonalClient','RunClient','RunRequest','RunRecord','RunEvent','RunEventPage','RunFinalAnswer','RunHTTPError','AcceptanceUncertain']
