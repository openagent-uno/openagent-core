from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.prompts import PromptBlock

PROMPT=PromptBlock("module.workflows","2", """## Workflows

Use `workflows` for multi-step pipelines: distinct stages, branching,
conditions and data passed between steps. Work a single timed prompt can do
belongs in `schedules` when that source is listed. Create and change
definitions only through its tools. A run keeps the exact definition revision
and tool references it started with; if a destination is unavailable the run
fails instead of switching to another tool.""",
"openagent-module-workflows", section="automation", priority=20)

descriptor=NativeCapabilityDescriptor(id="workflows",version="1.1.0b2",
    native_sources=("workflow-manager",),source_names={"workflow-manager":"workflows"},
    search_operation="list_workflows",search_source="workflows",
    search_accepts_limit=False,
    additional_prompt_blocks=(PROMPT,),optional_integrations=frozenset({"scheduler","events"}),
    required_services=frozenset({"automation_management"}))
__all__=["descriptor"]
