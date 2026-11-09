"""Tool-free, stateless inference for classifiers and bounded transformations.

This API is separate from agent runs. It cannot receive a session, capability
catalog or executor and never chooses a framework prompt on the host's behalf.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Protocol
import copy
import json


MAX_INFERENCE_IMAGES = 24
MAX_INFERENCE_IMAGE_BYTES = 8 * 1024**2
MAX_INFERENCE_SCHEMA_BYTES = 256 * 1024


@dataclass(frozen=True, slots=True)
class InferenceImage:
    """An in-memory image already admitted by the trusted caller."""

    content: bytes
    mime_type: str = 'image/jpeg'

    def __post_init__(self):
        if (not isinstance(self.content, bytes) or not 0 < len(self.content) <= MAX_INFERENCE_IMAGE_BYTES
                or self.mime_type not in {'image/jpeg', 'image/png', 'image/webp'}):
            raise ValueError('Inference requires a bounded JPEG, PNG or WebP image')


@dataclass(frozen=True,slots=True)
class InferenceRequest:
    messages: tuple[Mapping[str,Any],...]
    instructions: str = ''
    images: tuple[InferenceImage, ...] = ()
    response_schema: Mapping[str, Any] | None = None

    def __post_init__(self):
        if not isinstance(self.instructions,str):
            raise ValueError('Inference instructions must be text')
        if not isinstance(self.messages,tuple) or not self.messages:
            raise ValueError('Inference requires an immutable nonempty message sequence')
        for message in self.messages:
            if set(message)-{'role','content'} or message.get('role') not in {'user','assistant'}:
                raise ValueError('Only conversational text is accepted by stateless inference')
            if not isinstance(message.get('content'),str):raise ValueError('Inference content must be text')
        if (not isinstance(self.images, tuple) or len(self.images) > MAX_INFERENCE_IMAGES
                or any(not isinstance(image, InferenceImage) for image in self.images)
                or sum(len(image.content) for image in self.images) > MAX_INFERENCE_IMAGE_BYTES):
            raise ValueError('Inference images exceed the media limit')
        if self.images and not any(message['role'] == 'user' for message in self.messages):
            raise ValueError('Inference images require a user message')
        if self.response_schema is not None:
            if not isinstance(self.response_schema, Mapping):
                raise ValueError('The inference response schema must be a JSON object')
            try:
                schema = json.dumps(dict(self.response_schema), allow_nan=False, ensure_ascii=False,
                                    separators=(',', ':')).encode()
            except (TypeError, ValueError, RecursionError):
                raise ValueError('The inference response schema must be a JSON object') from None
            if len(schema) > MAX_INFERENCE_SCHEMA_BYTES:
                raise ValueError('The inference response schema exceeds its byte limit')


class InferenceProvider(Protocol):
    async def infer(self,request: InferenceRequest) -> Any: ...


class StatelessInference:
    def __init__(self,provider: InferenceProvider):self.provider=provider

    async def complete(self,request: InferenceRequest):
        schema = copy.deepcopy(dict(request.response_schema)) if request.response_schema is not None else None
        snapshot = InferenceRequest(copy.deepcopy(request.messages), request.instructions, request.images, schema)
        return await self.provider.infer(snapshot)
