from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from aiohttp import web
from openagent_core.inference import InferenceRequest,StatelessInference
from openagent_core.engine import NativeProvider


class StatelessInferenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_provider_http_has_no_tools_history_or_database(self):
        received=[]
        async def complete(request):
            body=await request.json();received.append(body)
            self.assertFalse(body.get('tools'))
            return web.json_response({'id':'reply','object':'chat.completion','created':1,'model':'fixture',
                'choices':[{'index':0,'message':{'role':'assistant','content':'category-a'},'finish_reason':'stop'}],
                'usage':{'prompt_tokens':4,'completion_tokens':2,'total_tokens':6}})
        app=web.Application();app.router.add_post('/v1/chat/completions',complete)
        runner=web.AppRunner(app);await runner.setup();site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
        port=site._server.sockets[0].getsockname()[1]
        with tempfile.TemporaryDirectory() as temporary:
            provider=NativeProvider('openai:fixture',api_key='fixture-only',base_url=f'http://127.0.0.1:{port}/v1')
            try:
                with patch.object(provider,'_ensure_agent',side_effect=AssertionError('inference is not an agent run')),patch.object(provider,'_ensured_runtime_db_path',side_effect=AssertionError('inference has no storage')):
                    result=await StatelessInference(provider).complete(InferenceRequest(({'role':'user','content':'Classify this'},),'Return the category'))
                self.assertEqual(result.content,'category-a');self.assertEqual(result.input_tokens,4)
                self.assertIn(received[0]['messages'][0]['role'],{'system','developer'})
                self.assertEqual(received[0]['messages'][0]['content'],'Return the category')
                self.assertEqual(received[0]['messages'][1]['role'],'user')
                self.assertEqual(list(Path(temporary).iterdir()),[])
            finally:
                await provider.shutdown();await runner.cleanup()


if __name__=='__main__':unittest.main()
