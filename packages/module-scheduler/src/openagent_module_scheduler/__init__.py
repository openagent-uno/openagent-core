from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.prompts import PromptBlock

PROMPT=PromptBlock("module.scheduler","2", """## Scheduled work

Use `schedules` for a prompt or registered target that should run at a time
or on a recurrence. Each firing starts a fresh session, unless the task is
created with `in_this_conversation=true` to run inside this conversation (a
loop that sees and answers in this chat). Set the timezone explicitly, preview
before creating, and verify the created task before saying it exists. Never
create recurring work with crontab, launchd, systemd, at jobs, self-restarting
shell commands, the model backend's own scheduler or a third-party scheduler:
those are invisible to the user and bypass identity, authorization and
cancellation. Never promise a future action without a verified schedule.""",
"openagent-module-scheduler", section="automation", priority=10)

descriptor=NativeCapabilityDescriptor(id="scheduler",version="1.1.0b2",
    native_sources=("scheduler",),source_names={"scheduler":"schedules"},
    search_operation="list_scheduled_tasks",search_source="schedules",
    search_accepts_limit=False,
    additional_prompt_blocks=(PROMPT,),optional_integrations=frozenset({"workflows"}),
    required_services=frozenset({"automation_management"}))
__all__=["descriptor"]
