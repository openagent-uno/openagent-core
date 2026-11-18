"""Subscription-only hosts must not require the optional reasoning engine."""
import os
from pathlib import Path
import subprocess
import sys


def test_subscription_services_do_not_import_engine_dependencies():
    root = Path(__file__).resolve().parents[2]
    script = r'''
import asyncio, importlib.abc, sys, tempfile
from pathlib import Path
blocked = {'yaml', 'aiosqlite', 'sqlalchemy', 'mcp', 'rich', 'jinja2',
           'croniter', 'regex', 'jsonschema', 'openai', 'anthropic', 'groq'}
class NoEngine(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in blocked:
            raise ModuleNotFoundError('Unexpected optional engine import: ' + fullname)
sys.meta_path.insert(0, NoEngine())
from openagent_core.subscriptions.service import SubscriptionService
from openagent_core.subscriptions.host import create_app
from openagent_core.models.connections import subscription_model_id
assert subscription_model_id('anthropic', 'sonnet-4-6') == 'claude-sonnet-4-6'
async def check():
    with tempfile.TemporaryDirectory() as temporary:
        for provider in ('claude', 'codex'):
            service = SubscriptionService(provider, Path(temporary) / (provider + '.json'))
            try:
                app = create_app(service, api_key='isolated-inference', admin_api_key='isolated-admin')
                assert app is not None
            finally:
                await service.close()
asyncio.run(check())
assert not any(name.split('.')[0] in blocked for name in sys.modules)
assert 'openagent_core.models.native_provider' not in sys.modules
'''
    result = subprocess.run([sys.executable, '-B', '-c', script],
                            env={**os.environ, 'PYTHONPATH': str(root / 'packages/runtime/src')},
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
