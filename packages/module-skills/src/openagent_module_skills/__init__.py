from openagent_core.module_adapters import NativeCapabilityDescriptor

descriptor = NativeCapabilityDescriptor(
    id='skills', version="1.1.0b1", native_sources=('skills', 'skill-data'),
    source_names={'skills': 'skills', 'skill-data': 'skill-data'}, prompt_rule_ids=(),
)

__all__ = ["descriptor"]
