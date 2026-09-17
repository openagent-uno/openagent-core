from openagent_core.module_adapters import NativeCapabilityDescriptor

descriptor = NativeCapabilityDescriptor(
    id='budget', version="1.1.0b1", native_sources=('budget-manager',),
    source_names={'budget-manager': 'budget'}, prompt_rule_ids=(),
)

__all__ = ["descriptor"]
