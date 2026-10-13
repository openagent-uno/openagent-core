"""Admission for Vault paths whose historical copies are not privacy attested.

Audit trailers and model arguments never supply content origins. The canonical
publisher validates the exact admitted execution; a protected run cannot mutate
an unqualified Vault (including its Git history, index and derived copies).
Human management outside a run remains available.
"""
from __future__ import annotations

from openagent_core.content_privacy import PrivacyConflict, PrivacyFence, ToolResultPrivacyPublisher


async def require_unprotected_execution(surface: str) -> None:
    from openagent_core.runtime import current_execution_context, current_run_id, current_runtime
    runtime, context, run_id = current_runtime(), current_execution_context(), current_run_id()
    if run_id is None:
        return
    if runtime is None or context is None:
        raise PrivacyConflict("A canonical writer requires the exact execution context")
    publisher = runtime.service(ToolResultPrivacyPublisher)
    if publisher is None:
        # A runtime without a provenance publisher cannot admit protected
        # connector results in the first place. Preserve ordinary embeddings.
        return
    if await publisher.origins_for_execution(context, run_id):
        raise PrivacyFence(f"{surface} has no attested writer covering historical copies")
