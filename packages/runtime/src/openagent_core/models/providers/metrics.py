"""Provider-side metric re-exports.

Lets per-provider drivers (anthropic, openai, etc.) import the metrics
types via ``openagent_core.models.providers.metrics`` without reaching across into
``openagent_core.core.metrics`` directly. Pure re-export shim.
"""

from openagent_core.core.metrics import (  # noqa: F401
    BaseMetrics,
    MessageMetrics,
    Metrics,
    ModelMetrics,
    ModelType,
    RunMetrics,
    SessionMetrics,
    ToolCallMetrics,
    accumulate_eval_metrics,
    accumulate_model_metrics,
    merge_background_metrics,
)

# Explicit re-export for type checkers
__all__ = [
    "BaseMetrics",
    "MessageMetrics",
    "Metrics",
    "ModelMetrics",
    "ModelType",
    "RunMetrics",
    "SessionMetrics",
    "ToolCallMetrics",
    "accumulate_eval_metrics",
    "accumulate_model_metrics",
    "merge_background_metrics",
]
