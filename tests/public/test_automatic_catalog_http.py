"""Auto's normal dispatcher, SDK and SQLite route a refused primary to backup."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from openagent_core.engine import MemoryDB, ModelDispatcher
from openagent_core.models.providers.fallback import reset_breaker


@pytest.mark.asyncio
async def test_automatic_text_recovers_with_actual_account_label_and_usage(tmp_path):
    seen=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            payload=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            key=self.headers.get('Authorization');seen.append(key)
            limited=key=='Bearer limited-fixture'
            value=({'error':{'type':'rate_limit_error','message':'quota exhausted'}} if limited else
                   {'id':'fixture','object':'chat.completion','created':1,'model':'fixture-text',
                    'choices':[{'index':0,'message':{'role':'assistant','content':'4503'},'finish_reason':'stop'}],
                    'usage':{'prompt_tokens':100,'completion_tokens':5,'total_tokens':105}})
            data=json.dumps(value).encode();self.send_response(429 if limited else 200)
            self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)))
            self.end_headers();self.wfile.write(data)
    reset_breaker();server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    db=MemoryDB(str(tmp_path/'state.sqlite'));await db.connect()
    dispatcher=None
    try:
        for index,(name,key) in enumerate([('primary','limited-fixture'),('backup','healthy-fixture')]):
            pid=await db.upsert_provider(name=name,framework='api-based',api_key=key,
                base_url=f'http://127.0.0.1:{server.server_port}/v1')
            await db.upsert_model(provider_id=pid,model='fixture-text',is_classifier=index==0)
        dispatcher=ModelDispatcher(providers_config=await db.materialise_providers_config())
        dispatcher.set_db(db)
        result=await dispatcher.generate([{'role':'user','content':'237 × 19'}],session_id='fixture-auto')
        assert '4503' in result.content
        assert result.model=='backup:fixture-text'
        assert seen[:2]==['Bearer limited-fixture','Bearer healthy-fixture']
        # The optional history-summary manager is a separate exact-model call.
        assert all(key=='Bearer limited-fixture' for key in seen[2:])
        assert len(seen)<=5
        rows=await (await db._conn.execute('select model,sum(input_tokens),sum(output_tokens) from usage_log group by model')).fetchall()
        assert [tuple(row) for row in rows]==[('backup:fixture-text',100,5)]
    finally:
        if dispatcher is not None:await dispatcher.shutdown()
        await db.close();server.shutdown();server.server_close();thread.join(timeout=2);reset_breaker()
