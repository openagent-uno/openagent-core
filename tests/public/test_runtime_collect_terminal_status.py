import asyncio
from types import SimpleNamespace
import pytest
from openagent_core.models.dispatcher import _arun_runtime_collect
from openagent_core.core.public_errors import RunTurnError
from openagent_core.core._run_state.base import RunStatus


class Runtime:
    def __init__(self,status,content):
        self.output=SimpleNamespace(status=status,content=content,tools=[],metrics=None)
    async def arun(self,*args,**kwargs):
        return self.output


async def collect(status,content='PROVIDER DIAGNOSTIC'):
    return await _arun_runtime_collect(Runtime(status,content),prompt='human',session_id='session',
        user_id='user',error_event='test.error',entry_runtime_id='model')


@pytest.mark.asyncio
@pytest.mark.parametrize('status',[RunStatus.error,'ERROR','error','FAILED','failed'])
async def test_error_status_never_published_as_success(status):
    with pytest.raises(RunTurnError,match='PROVIDER DIAGNOSTIC'):
        await collect(status)


@pytest.mark.asyncio
@pytest.mark.parametrize('status',[RunStatus.cancelled,'CANCELLED','cancelled','CANCELED'])
async def test_cancel_status_propagates_without_response_or_retry(status):
    with pytest.raises(asyncio.CancelledError,match='PROVIDER DIAGNOSTIC'):
        await collect(status)


@pytest.mark.asyncio
@pytest.mark.parametrize('status',[RunStatus.completed,'COMPLETED',None])
async def test_success_and_legacy_missing_status_preserve_normal_response(status):
    assert (await collect(status,'MODEL ANSWER')).content=='MODEL ANSWER'


@pytest.mark.asyncio
async def test_collected_runtime_error_preserves_type_when_content_has_no_category():
    from openagent_core.core.public_errors import classify_run_error
    runtime = Runtime('ERROR', 'PRIVATE-DIAGNOSTIC')
    runtime.output.error_type = 'ModelRateLimitError'
    with pytest.raises(RunTurnError) as raised:
        await _arun_runtime_collect(runtime, prompt='hello', session_id='s', user_id='u',
            error_event='test.error', entry_runtime_id='model')
    assert classify_run_error(raised.value).code == 'rate_limit'
