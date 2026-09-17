from openagent_core.module_adapters import NativeCapabilityDescriptor

descriptor = NativeCapabilityDescriptor(
    id='logs', version="1.1.0b1", native_sources=('logs',),
    source_names={'logs': 'logs'}, prompt_rule_ids=(),
)

__all__ = ["descriptor"]
