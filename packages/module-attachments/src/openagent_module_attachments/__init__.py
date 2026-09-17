from openagent_core.module_adapters import NativeCapabilityDescriptor

descriptor = NativeCapabilityDescriptor(
    id='attachments', version="1.1.0b1", native_sources=('attachments',),
    source_names={'attachments': 'attachments'}, prompt_rule_ids=('module.attachments',),
)

__all__ = ["descriptor"]
