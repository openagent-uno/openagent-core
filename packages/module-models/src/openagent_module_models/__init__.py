from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.contracts import ModelCatalog

descriptor = NativeCapabilityDescriptor(
    id='models', version="1.1.0b1", native_sources=('model-manager',),
    source_names={'model-manager': 'models'}, prompt_rule_ids=('module.models',),
    required_services=frozenset({ModelCatalog}),
)

__all__ = ["descriptor"]
