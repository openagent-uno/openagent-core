from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.contracts import DelegationService

descriptor = NativeCapabilityDescriptor(
    id='delegation', version="1.1.0b1", native_sources=('delegation',),
    source_names={'delegation': 'delegation'}, prompt_rule_ids=('module.delegation',),
    required_services=frozenset({DelegationService}),
)

__all__ = ["descriptor"]
