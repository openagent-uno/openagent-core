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
from urllib.parse import quote, urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler
from openagent_core import RunRequest, RunRecord, RunEvent


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


class RunClient:
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

    async def submit(self,request: RunRequest) -> RunRecord:
        try:
            value=await self._request('POST',body=asdict(request))
        except RunHTTPError as exc:
            if exc.status >= 500:
                raise AcceptanceUncertain(request.run_id) from exc
            raise
        except (URLError,TimeoutError,ConnectionError,OSError,json.JSONDecodeError) as exc:
            raise AcceptanceUncertain(request.run_id) from exc
        return RunRecord(**value)

    async def get(self,run_id: str) -> RunRecord:
        return RunRecord(**await self._request('GET','/'+quote(run_id,safe='')))

    async def events(self,run_id: str,after: int=0) -> tuple[RunEvent,...]:
        if after<0: raise ValueError('Cursor cannot be negative')
        result=await self._request('GET','/'+quote(run_id,safe='')+'/events?after='+str(after))
        return tuple(RunEvent(**event) for event in result['events'])

    async def cancel(self,run_id: str) -> RunRecord:
        return RunRecord(**await self._request('POST','/'+quote(run_id,safe='')+'/cancel'))

    async def children(self,run_id: str) -> tuple[RunRecord,...]:
        result=await self._request('GET','/'+quote(run_id,safe='')+'/children')
        return tuple(RunRecord(**row) for row in result['runs'])

    async def wait(self,run_id: str,*,interval: float=.5) -> RunRecord:
        if interval<=0: raise ValueError('Polling interval must be positive')
        while True:
            result=await self.get(run_id)
            if result.terminal: return result
            await asyncio.sleep(interval)


__all__=['RunClient','RunRequest','RunRecord','RunEvent','RunHTTPError','AcceptanceUncertain']
