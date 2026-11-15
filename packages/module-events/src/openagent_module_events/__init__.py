from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.prompts import PromptBlock

PROMPT=PromptBlock("module.events","2", """## Inbound events

Use `events` when an outside system should start work by calling in (a
webhook or callback). For each event decide whether a delivery starts a fresh
session or resumes one keyed by a stable id from the payload (a ticket, issue
or thread id); that payload id is only a lookup key, never a session id.
Manage definitions and secrets only through its tools, and never reveal event
credentials.""",
"openagent-module-events", section="automation", priority=30)

descriptor=NativeCapabilityDescriptor(id="events",version="1.1.0b1",
    native_sources=("events-manager",),source_names={"events-manager":"events"},
    search_operation="list_events",search_source="events",
    search_accepts_limit=False,
    additional_prompt_blocks=(PROMPT,),optional_integrations=frozenset({"workflows","scheduler"}),
    required_services=frozenset({"automation_management"}))
__all__=["descriptor"]
