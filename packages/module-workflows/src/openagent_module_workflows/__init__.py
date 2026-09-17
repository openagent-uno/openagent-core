from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.prompts import PromptBlock

PROMPT=PromptBlock("module.workflows","1", """## Workflows

Use the `workflows` capability for durable multi-step pipelines, branching,
conditionals and data flowing between distinct stages. Use its canonical
management operations instead of editing definitions or database rows. A
workflow run keeps the exact authorized definition revision and tool references;
an unavailable destination must fail rather than fall back to another tool.""",
"openagent-module-workflows")

descriptor=NativeCapabilityDescriptor(id="workflows",version="1.1.0b1",
    native_sources=("workflow-manager",),source_names={"workflow-manager":"workflows"},
    search_operation="list_workflows",search_source="workflows",
    search_accepts_limit=False,
    additional_prompt_blocks=(PROMPT,),optional_integrations=frozenset({"scheduler","events"}),
    required_services=frozenset({"automation_management"}))
__all__=["descriptor"]
