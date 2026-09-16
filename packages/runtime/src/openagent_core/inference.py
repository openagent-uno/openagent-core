"""Tool-free, stateless inference for classifiers and bounded transformations.

This API is separate from agent runs. It cannot receive a session, capability
catalog or executor and never chooses a framework prompt on the host's behalf.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Protocol
import copy


@dataclass(frozen=True,slots=True)
class InferenceRequest:
    messages: tuple[Mapping[str,Any],...]
    instructions: str = ''

    def __post_init__(self):
        if not isinstance(self.instructions,str):
            raise ValueError('Inference instructions must be text')
        if not isinstance(self.messages,tuple) or not self.messages:
            raise ValueError('Inference requires an immutable nonempty message sequence')
        for message in self.messages:
            if set(message)-{'role','content'} or message.get('role') not in {'user','assistant'}:
                raise ValueError('Only conversational text is accepted by stateless inference')
            if not isinstance(message.get('content'),str):raise ValueError('Inference content must be text')


class InferenceProvider(Protocol):
    async def infer(self,request: InferenceRequest) -> Any: ...


class StatelessInference:
    def __init__(self,provider: InferenceProvider):self.provider=provider

    async def complete(self,request: InferenceRequest):
        snapshot = InferenceRequest(copy.deepcopy(request.messages), request.instructions)
        return await self.provider.infer(snapshot)
