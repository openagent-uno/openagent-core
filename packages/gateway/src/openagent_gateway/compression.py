"""Negotiated compression of buffered JSON; streaming protocols stay streaming."""
from __future__ import annotations

import gzip
from aiohttp import web
from openagent_core.persistence import run_sync


def accepts_gzip(value: str) -> bool:
    weights = {}
    for item in value.lower().split(','):
        coding, *parameters = item.strip().split(';')
        weight = 1.0
        for parameter in parameters:
            name, separator, raw = parameter.strip().partition('=')
            if name == 'q' and separator:
                try: weight = float(raw)
                except ValueError: weight = 0.0
        weights[coding] = weight if 0 <= weight <= 1 else 0.0
    return weights.get('gzip', weights.get('*', 0.0)) > 0


@web.middleware
async def json_compression(request, handler):
    """Compress large JSON for capable HTTP/Iroh clients without blocking IO.

    No negotiation means identity. Prepared responses, ranges, validators,
    no-transform and pre-encoded bodies retain their original representation.
    """
    response = await handler(request)
    if (request.method != 'GET' or request.headers.get('Range')
            or not isinstance(response, web.Response) or response.prepared
            or response.status != 200 or not isinstance(response.body, bytes)
            or not 8192 <= len(response.body) <= 64 << 20
            or not (response.content_type == 'application/json' or response.content_type.endswith('+json'))
            or any(name in response.headers for name in ('Content-Encoding', 'Content-Range', 'ETag'))
            or 'no-transform' in response.headers.get('Cache-Control', '').lower()):
        return response
    vary = response.headers.get('Vary', '')
    if not any(value.strip().lower() in ('accept-encoding', '*') for value in vary.split(',')):
        response.headers['Vary'] = (vary + ', ' if vary else '') + 'Accept-Encoding'
    if not accepts_gzip(request.headers.get('Accept-Encoding', '')):
        return response
    body = await run_sync(gzip.compress, response.body, compresslevel=1, mtime=0)
    if len(body) >= len(response.body):
        return response
    headers = response.headers.copy()
    headers.popall('Content-Length', None)
    headers['Content-Encoding'] = 'gzip'
    compressed = web.Response(body=body, status=response.status, reason=response.reason, headers=headers)
    compressed.cookies.update(response.cookies)
    if response.keep_alive is False:
        compressed.force_close()
    return compressed
