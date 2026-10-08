import asyncio
from types import SimpleNamespace

import pytest

from openagent_core.automation import build_automation_toolkit
from openagent_core.contracts import ExecutionContext, PrincipalRef
from openagent_core.runtime import execution_scope


class _Management:
    def __init__(self):
        self.calls = []

    async def review_authorization(self, kind, definition_id, context):
        self.calls.append(("review", kind, definition_id, context))
        return {"digest": "abc", "authorized": False}

    async def approve_authorization(self, kind, definition_id, digest, context):
        self.calls.append(("approve", kind, definition_id, digest, context))
        return {"digest": digest, "authorized": True}


@pytest.mark.parametrize(
    ("kind", "review_name", "approve_name"),
    (
        ("scheduled_task", "review_scheduled_task_authorization", "approve_scheduled_task_authorization"),
        ("workflow", "review_workflow_authorization", "approve_workflow_authorization"),
        ("event", "review_event_authorization", "approve_event_authorization"),
    ),
)
def test_automation_toolkit_exposes_exact_revision_review(
    kind, review_name, approve_name,
):
    management = _Management()
    runtime = SimpleNamespace(
        services=SimpleNamespace(automation_management=management),
    )
    principal = PrincipalRef("openagent", "network", "alice")
    context = ExecutionContext(
        principal, principal, principal, "session", "agent", (principal,),
        ingress_id="verified",
    )
    toolkit = build_automation_toolkit(kind)

    assert review_name in toolkit.async_functions
    assert approve_name in toolkit.async_functions
    async def exercise():
        with execution_scope(runtime, context, "run"):
            review = await toolkit.async_functions[review_name].entrypoint("definition")
            approval = await toolkit.async_functions[approve_name].entrypoint(
                "definition", "abc",
            )
        return review, approval

    review, approval = asyncio.run(exercise())

    assert review == {"digest": "abc", "authorized": False}
    assert approval == {"digest": "abc", "authorized": True}
    assert management.calls == [
        ("review", kind, "definition", context),
        ("approve", kind, "definition", "abc", context),
    ]
