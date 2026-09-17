from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.code_execution import CodeExecutor

descriptor = NativeCapabilityDescriptor(
    id='ptc', version="1.1.0b1", native_sources=('ptc',),
    source_names={'ptc': 'ptc'}, prompt_rule_ids=(),
    required_services=frozenset({CodeExecutor}),
)

__all__ = ["descriptor"]
