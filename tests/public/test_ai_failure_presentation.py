"""Only stable, actionable AI failure categories cross the durable boundary."""
import pytest
from openagent_core import Runtime,RuntimeServices,RuntimeSettings,PrincipalRef,ExecutionContext,RunRequest
from openagent_core.core.public_errors import classify_run_error, PUBLIC_MESSAGES, RunTurnError
from openagent_core.core.runtime_errors import ModelAuthenticationError,ModelRateLimitError
from openagent_storage_sqlite import SqliteRuntimeStore

@pytest.mark.parametrize('error,code',[
 (ModelAuthenticationError('API key invalid: PRIVATE-KEY'),'auth'),
 (ModelAuthenticationError('refresh token expired PRIVATE-TOKEN'),'auth_expired'),
 (ModelRateLimitError('insufficient_quota PRIVATE-KEY'),'quota_exhausted'),
 (ModelRateLimitError('requests per minute exceeded'),'rate_limit'),
 (RuntimeError('no_enabled_model'),'model_not_configured'),
 (RuntimeError('provider_not_configured'),'provider_not_configured'),
 (RuntimeError('api_key must be set'),'provider_not_configured'),
 (RuntimeError('credit balance is too low'),'quota_exhausted'),
 (RuntimeError('context_length_exceeded'),'context_length'),
 (RunTurnError('invalid_grant PRIVATE-TOKEN'),'auth_expired'),
 (RunTurnError('provider rate limit'),'rate_limit'),
 (LookupError('Pinned model is unavailable'),'model_unavailable'),
 (PermissionError('The pinned model is currently excluded by its budget'),'budget_exhausted'),
 (RuntimeError('PRIVATE-KEY unknown backend failure'),'generic'),
])
def test_classification_is_actionable_and_never_quotes_diagnostics(error,code):
 failure=classify_run_error(error)
 assert failure.code==code
 assert failure.public_message==PUBLIC_MESSAGES[code]
 assert 'PRIVATE' not in failure.public_message
 assert len(failure.public_message)<300

def test_wrapped_expiration_and_cyclic_causes_are_bounded():
 wrapped=RuntimeError('provider failed');wrapped.__cause__=ModelAuthenticationError('token expired PRIVATE-TOKEN')
 assert classify_run_error(wrapped).code=='auth_expired'
 wrapped.__cause__=wrapped
 assert classify_run_error(wrapped).code=='generic'

@pytest.mark.asyncio
async def test_durable_failed_run_retains_safe_category_after_restart(tmp_path):
 class Policy:
  async def authorize(self,*args,**kwargs):return True
 class Failing:
  async def execute(self,*args):raise ModelAuthenticationError('token expired PRIVATE-TOKEN')
 p=PrincipalRef('host','tenant','alice');ctx=ExecutionContext(p,p,p,'session','agent',(p,))
 runtime=Runtime(RuntimeSettings('agent',tmp_path),RuntimeServices(SqliteRuntimeStore(tmp_path/'state.sqlite3'),Failing(),Policy()))
 await runtime.start()
 try:
  await runtime.submit(RunRequest('run','session','request','hello'),ctx)
  record=await runtime.wait('run',ctx)
  assert record.status=='failed'
  assert record.output=={'error_type':'ModelAuthenticationError','error_code':'auth_expired','error_public':PUBLIC_MESSAGES['auth_expired']}
 finally:await runtime.close()
 runtime=Runtime(RuntimeSettings('agent',tmp_path),RuntimeServices(SqliteRuntimeStore(tmp_path/'state.sqlite3'),Failing(),Policy()))
 await runtime.start()
 try:
  record=await runtime.get_run('run',ctx)
  assert record.output['error_code']=='auth_expired'
  assert 'PRIVATE' not in str(record.output)
 finally:await runtime.close()
