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


@pytest.fixture
def context():
    principal = PrincipalRef("openagent", "network", "member")
    return ExecutionContext(principal, principal, principal, "session", "agent", (principal,), ingress_id="verified")


@pytest.mark.parametrize(
    ("kind", "review_name", "approve_name"),
    (
        ("scheduled_task", "review_scheduled_task_authorization", "approve_scheduled_task_authorization"),
        ("workflow", "review_workflow_authorization", "approve_workflow_authorization"),
        ("event", "review_event_authorization", "approve_event_authorization"),
    ),
)
def test_automation_toolkit_exposes_exact_revision_review(
    kind, review_name, approve_name, context,
):
    management = _Management()
    runtime = SimpleNamespace(
        services=SimpleNamespace(automation_management=management),
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


@pytest.mark.asyncio
@pytest.mark.parametrize("sources", (["source-b", "source-a"], []))
async def test_approval_tool_forwards_the_exact_source_selection(sources, context):
    calls = []

    async def approve(kind, definition, digest, caller, *, sources):
        calls.append((kind, definition, digest, caller, sources))
        return {"authorized": True}

    management = SimpleNamespace(approve_authorization=approve)
    runtime = SimpleNamespace(services=SimpleNamespace(automation_management=management))
    tool = build_automation_toolkit("scheduled_task").async_functions["approve_scheduled_task_authorization"]
    tool.process_entrypoint()
    assert {"type": "array", "items": {"type": "string"}} in tool.parameters["properties"]["sources"]["anyOf"]
    assert "sources" not in tool.parameters["required"]

    with execution_scope(runtime, context, "run"):
        assert await tool.entrypoint("definition", "abc", sources=sources) == {"authorized": True}
    assert calls == [("scheduled_task", "definition", "abc", context, sources)]


@pytest.mark.asyncio
async def test_unsupported_approval_cannot_widen_a_source_selection(context):
    toolkit = build_automation_toolkit("scheduled_task")
    review = toolkit.async_functions["review_scheduled_task_authorization"].entrypoint
    approve = toolkit.async_functions["approve_scheduled_task_authorization"].entrypoint
    services = SimpleNamespace(automation_management=SimpleNamespace())
    with execution_scope(SimpleNamespace(services=services), context, "run"):
        with pytest.raises(LookupError, match="authorization review"):
            await review("definition")
        with pytest.raises(LookupError, match="authorization approval"):
            await approve("definition", "abc")
        legacy = _Management()
        services.automation_management = legacy
        with pytest.raises(LookupError, match="source selection"):
            await approve("definition", "abc", sources=[])
        assert legacy.calls == []
        assert await approve("definition", "abc", sources=None) == {"digest": "abc", "authorized": True}
        assert legacy.calls == [("approve", "scheduled_task", "definition", "abc", context)]

        async def ignores_keywords(*args, **kwargs):
            pytest.fail("An explicit selection must never reach a host that can silently ignore it")

        services.automation_management = SimpleNamespace(approve_authorization=ignores_keywords)
        with pytest.raises(LookupError, match="source selection"):
            await approve("definition", "abc", sources=["source-a"])
