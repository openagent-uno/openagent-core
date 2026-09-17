from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.prompts import PromptBlock

PROMPT=PromptBlock("module.events","1", """## Inbound events

Use the `events` capability when an external system should start authorized work.
Choose explicitly whether each delivery creates a fresh session or resumes one
through a stable external binding key. External payload identifiers are lookup
keys, never OpenAgent session IDs. Manage definitions and secrets through the
module service; never expose event credentials or edit its storage directly.""",
"openagent-module-events")

descriptor=NativeCapabilityDescriptor(id="events",version="1.1.0b1",
    native_sources=("events-manager",),source_names={"events-manager":"events"},
    search_operation="list_events",search_source="events",
    search_accepts_limit=False,
    additional_prompt_blocks=(PROMPT,),optional_integrations=frozenset({"workflows","scheduler"}),
    required_services=frozenset({"automation_management"}))
__all__=["descriptor"]
