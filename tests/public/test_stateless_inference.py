from pathlib import Path
import base64
import tempfile
import unittest
from unittest.mock import patch
from aiohttp import web
from openagent_core.inference import InferenceImage,InferenceRequest,StatelessInference
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
                    inference = StatelessInference(provider)
                    result=await inference.complete(InferenceRequest(({'role':'user','content':'Classify this'},),'Return the category'))
                    sample = b'\xff\xd8approved-jpeg\xff\xd9'
                    await inference.complete(InferenceRequest(({'role':'user','content':'Inspect this'},),
                        'Return the category', (InferenceImage(sample),)))
                self.assertEqual(result.content,'category-a');self.assertEqual(result.input_tokens,4)
                self.assertIn(received[0]['messages'][0]['role'],{'system','developer'})
                self.assertEqual(received[0]['messages'][0]['content'],'Return the category')
                self.assertEqual(received[0]['messages'][1]['role'],'user')
                image = received[1]['messages'][-1]['content'][-1]
                self.assertEqual(image['image_url']['url'], 'data:image/jpeg;base64,' + base64.b64encode(sample).decode())
                self.assertEqual(len(received[1]['messages']), 2)
                schema = {'type': 'object', 'properties': {'category': {'type': 'string'}},
                          'required': ['category'], 'additionalProperties': False}
                request = InferenceRequest(({'role':'user','content':'Classify this'},),
                                           'Return the category', response_schema=schema)
                await inference.complete(request)
                self.assertNotIn('response_format', received[-1])
                structured = NativeProvider('openai:fixture', api_key='fixture-only',
                                            base_url=f'http://127.0.0.1:{port}/v1', stateless_json_schema=True)
                try:
                    await StatelessInference(structured).complete(request)
                    self.assertEqual(received[-1]['response_format'], {'type': 'json_schema', 'json_schema': {
                        'name': 'inference_output', 'strict': True, 'schema': schema,
                    }})
                finally:
                    await structured.shutdown()
                self.assertEqual(list(Path(temporary).iterdir()),[])
            finally:
                await provider.shutdown();await runner.cleanup()


if __name__=='__main__':unittest.main()
