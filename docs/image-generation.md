# Image generation contract

OpenAgent stores image models as `kind=image` in the same provider and model
catalog used by LLM, speech and transcription models. A host supplies the
provider key and base URL, enables the model, and authorizes the current turn.
Image models are never selected as the reasoning model for a session.

The default `media-gen` tool resolves an enabled image model, calls an
OpenAI-compatible `/v1/images/generations` endpoint through
`openagent_core.image_generation.generate_image_bytes`, validates the returned
PNG, JPEG or WebP bytes, and places the image in a local media cache. The model
can reuse that path in later tools. To send it through the current channel, it
includes the returned `[IMAGE:/absolute/path]` marker in the final reply. The
channel bridge turns the marker into a typed outbound attachment; the app's
session transcript also presents it as an image. Hosts may use the shared
transport with their own artifact repository and recipient policy instead.

`OPENAGENT_IMAGE_BASE_URL`, `OPENAGENT_IMAGE_API_KEY` and
`OPENAGENT_IMAGE_MODEL` remain explicit host overrides. A legacy LLM model with
`metadata.capabilities=["image_generation"]` remains supported. This lets a
subscription proxy expose a single chat model that can also draw. If the
catalog identity is qualified for chat routing, the host can set
`metadata.image_model_id` to the exact ID accepted by the provider's image
endpoint. Both the default tool and embedding hosts use the shared capability
and alias helpers. The conversational model can therefore be Claude while a
separate enabled image provider handles the image call; Claude's chat-only
proxy does not itself supply an image endpoint. Image editing and
provider-specific non-compatible APIs are outside this transport.

## Refusals

When the provider answers with an HTTP error, `generate_image_bytes` raises
`ImageGenerationError`. It carries the HTTP `status`, the provider's own error
type or code as `kind`, and a message that quotes what the provider said
(whitespace collapsed, at most 600 characters). The `media-gen` tool returns
that as `{ok: false, reason, status, error_type}`. The status alone does not
name the cause: a subscription service may answer 429 both for an exhausted
quota and for a sign-in that has ended, and only the quoted message and
`error_type` tell them apart. A model reading the tool result should report the
quoted cause.
