from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.prompts import PromptBlock

PROMPT=PromptBlock("module.scheduler","1", """## Scheduled work

Use the `schedules` capability for prompts or registered targets that fire at a
time or recurrence. Store timezone explicitly and use the module's preview and
management operations. Do not create recurring work through shell background
processes, crontab, launchd or systemd: such jobs bypass the product's identity,
authorization, visibility, cancellation and migration lifecycle. Never claim a
future action unless a schedule was durably created and its result verified.""",
"openagent-module-scheduler")

descriptor=NativeCapabilityDescriptor(id="scheduler",version="1.1.0b1",
    native_sources=("scheduler",),source_names={"scheduler":"schedules"},
    search_operation="list_scheduled_tasks",search_source="schedules",
    search_accepts_limit=False,
    additional_prompt_blocks=(PROMPT,),optional_integrations=frozenset({"workflows"}),
    required_services=frozenset({"automation_management"}))
__all__=["descriptor"]
