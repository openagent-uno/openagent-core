import gzip
import json
import threading

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer
from openagent_gateway.compression import accepts_gzip, json_compression


@pytest.mark.parametrize('value,expected', [('',False),('gzip',True),('br, gzip;q=0.5',True),
    ('gzip;q=0, *;q=1',False),('*;q=0.5',True),('gzip;q=nan',False),('gzip;q=2',False),
    ('gzip;q=bad',False),('identity',False)])
def test_explicit_compression_negotiation(value, expected):
    assert accepts_gzip(value) is expected


@pytest.mark.asyncio
async def test_wire_compression_preserves_json_cookies_headers_and_legacy_clients(monkeypatch):
    payload = {'messages': [{'role':'assistant', 'content':'fixture text ' * 100} for _ in range(100)]}
    original = json.dumps(payload).encode()
    loop_thread = threading.get_ident()
    compress = gzip.compress
    def off_thread(*args, **kwargs):
        assert threading.get_ident() != loop_thread
        return compress(*args, **kwargs)
    monkeypatch.setattr(gzip, 'compress', off_thread)
    async def handler(request):
        response = web.Response(body=original, content_type='application/json', headers={'X-Fixture':'preserved'})
        response.set_cookie('fixture', 'cookie', httponly=True)
        if request.query.get('etag'): response.headers['ETag'] = '"fixture"'
        if request.query.get('no_transform'): response.headers['Cache-Control'] = 'private, no-transform'
        return response
    app = web.Application(middlewares=[json_compression]); app.router.add_get('/', handler)
    async with TestServer(app) as server, ClientSession(auto_decompress=False, skip_auto_headers={'Accept-Encoding'}) as client:
        for headers, compressed in [({},False),({'Accept-Encoding':'gzip;q=0'},False),({'Accept-Encoding':'gzip'},True)]:
            async with client.get(server.make_url('/'), headers=headers) as response:
                body = await response.read()
                assert response.headers.get('Content-Encoding') == ('gzip' if compressed else None)
                assert (gzip.decompress(body) if compressed else body) == original
                assert response.headers['X-Fixture'] == 'preserved'
                assert response.cookies['fixture'].value == 'cookie'
                assert int(response.headers['Content-Length']) == len(body)
                assert 'Accept-Encoding' in response.headers['Vary']
                if compressed: assert len(body) < len(original) / 3
        for query, extra in [('?etag=1', {}), ('?no_transform=1', {}), ('', {'Range':'bytes=0-99'})]:
            async with client.get(server.make_url('/'+query),headers={'Accept-Encoding':'gzip', **extra}) as response:
                assert 'Content-Encoding' not in response.headers
                assert await response.read() == original


@pytest.mark.asyncio
async def test_prepared_event_streams_and_authorization_errors_are_unchanged():
    async def stream(request):
        response = web.StreamResponse(headers={'Content-Type':'text/event-stream'})
        await response.prepare(request)
        await response.write(b'data: fixture\n\n'); await response.write_eof()
        return response
    async def denied(request): return web.json_response({'error':'not_authorized', 'padding':'x' * 10000}, status=403)
    app = web.Application(middlewares=[json_compression])
    app.router.add_get('/events', stream); app.router.add_get('/denied', denied)
    async with TestServer(app) as server, ClientSession(auto_decompress=False) as client:
        async with client.get(server.make_url('/events')) as response:
            assert 'Content-Encoding' not in response.headers
            assert await response.read() == b'data: fixture\n\n'
        async with client.get(server.make_url('/denied')) as response:
            assert response.status == 403
            assert 'Content-Encoding' not in response.headers
