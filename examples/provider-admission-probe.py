"""One credential-scoped Chat Completions request with a synthetic prompt and a fresh ledger."""

import argparse
import asyncio
import json
import logging
import os
import time
from uuid import uuid4

from openagent_core import (
    CredentialResolver,
    ExecutionContext,
    PrincipalRef,
    ProviderAccount,
    ProviderAdmission,
    ProviderRoute,
    RunRequest,
    Runtime,
    RuntimeServices,
    RuntimeSettings,
)
from openagent_storage_postgres import PostgresRuntimeStore


async def check(model_id, endpoint):
    # Never pass secrets on the command line and never report upstream response text.
    secret = os.environ["OPENAGENT_PROBE_API_KEY"]
    dsn = os.environ["OPENAGENT_TEST_POSTGRES_DSN"]
    from openagent_core.models.providers.openai.chat import OpenAIChat
    from openagent_core.models.providers.message import Message
    from openagent_core.runtime import current_run_id

    schema = "oa_provider_probe_" + uuid4().hex
    ident = "probe-" + uuid4().hex
    principal = PrincipalRef("qualification", "synthetic", "operator")
    ctx = ExecutionContext(principal, principal, principal, ident, "probe", (principal,))
    nonce = uuid4().hex[:8]
    prompt = "Reply with exactly this eight-character code: " + nonce
    model = OpenAIChat(id=model_id, max_completion_tokens=64, retries=0)
    model._openagent_runtime_id = "openai:" + model_id

    class Probe:
        async def execute(self, request, context, runtime):
            answer = await model.aresponse(
                messages=[Message(role="user", content=prompt)], tools=[], tool_call_limit=0
            )
            return answer.content.strip() == nonce if isinstance(answer.content, str) else False

        async def authorize(self, context, action, resource, **options):
            return context == ctx and resource.tenant_id == ctx.tenant_id

        async def select(self, context, ref):
            if context != ctx or current_run_id() != ident or ref != model._openagent_runtime_id:
                raise PermissionError("Unknown model probe")
            return ProviderRoute(
                "qualification", 1, endpoint, (ProviderAccount("probe", "probe-token", 1),)
            )

        async def resolve(self, credential_ref, context):
            if context != ctx or current_run_id() != ident or credential_ref != "probe-token":
                raise PermissionError("Unknown qualification credential")
            return secret

    probe = Probe()
    ledger = PostgresRuntimeStore(dsn, agent_id="probe", schema=schema)
    services = RuntimeServices(ledger, probe, probe)
    services.registry.bind(ProviderAdmission, probe)
    services.registry.bind(CredentialResolver, probe)
    runtime = Runtime(RuntimeSettings("probe", require_provider_admission=True), services)
    start = time.monotonic()
    await runtime.start()
    try:
        await runtime.submit(RunRequest(ident, ident, ident, prompt, deadline_seconds=60), ctx)
        result = await asyncio.wait_for(runtime.wait(ident, ctx), 70)
        passed = result.status == "success" and result.output is True
        return {
            "passed": passed,
            "status": result.status,
            "duration_ms": round((time.monotonic() - start) * 1000),
        }
    finally:
        await runtime.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument(
        "--endpoint", required=True, help="Trusted API base URL without credentials"
    )
    args = parser.parse_args()
    logging.disable(logging.CRITICAL)
    try:
        receipt = asyncio.run(check(args.model, args.endpoint))
    except Exception as error:
        receipt = {"passed": False, "error_type": type(error).__name__}
    print(json.dumps(receipt))
    raise SystemExit(0 if receipt["passed"] else 1)
