<!-- Concrete host labels in this historical record are anonymized; original bytes and hashes are retained in the protected operator audit. -->
# Explicit host audio

`openagent_core.audio.VoiceService(db, fallback=None, worker_command=None, timeout_seconds=120)` resolves the latest enabled audio model using the existing provider/model IDs and metadata. `transcribe(path, language=None)` returns text or `None`; `synthesize(text, language=None)` returns `SynthesizedAudio(data, media_type)` or `None`.

Cloud audio uses the optional `audio` extra. Each invocation starts `python -I -m openagent_core.audio_worker`, passing only the operation, configured provider row and input over stdin. The worker gets an explicit minimum environment and temporary HOME, so LiteLLM's SDK globals, credential fallbacks and proxy environment cannot cross host instances. Keys never appear in arguments. The worker reserves a non-inheritable protocol descriptor, permanently routes ordinary stdout/stderr to the null device, and emits one bounded JSON result through that private descriptor. This isolation remains active through vendor background threads, cached logging handlers and interpreter/atexit shutdown; it does not end when the awaited provider call returns. The parent discards stderr. Cancellation and timeout terminate, then kill if necessary, and await the owned worker. No worker starts at import or on runtime startup.

A frozen product supplies `worker_command=(executable, '_audio-worker')` and dispatches that entrypoint directly to `audio_worker.main()` before normal startup. Local speech models are a host-provisioned `AudioFallback`, with the same two methods. Only an explicitly supplied fallback runs after an empty cloud result, cloud failure or timeout; task cancellation never starts fallback work. Core does not download local models or select an ambient cloud account.

Configured audio requires an explicit key and endpoint (registered OpenAI, Groq and ElevenLabs default endpoints are available); other vendors retain their LiteLLM adapter and require a configured endpoint. TTS sanitization and model metadata remain unchanged. Hosts authorize before invocation and again before publishing results.

Beta52 uses a lightweight HTTP adapter inside the same isolated worker for
`provider_name=openai` transcription. It calls only the configured
`base_url/audio/transcriptions`, passing the configured model, key, language
and prompt. It does not import the multi-provider SDK aggregator for this
route, avoiding expensive repeated cold imports on constrained hosts. Other
vendors and synthesis retain their existing adapters. The HTTP client ignores
ambient proxy configuration, refuses redirects, bounds input to 16 MiB and
the JSON response to 256 KiB, and retains the parent's cancellation/reaping
contract. Empty transcripts remain nullable; malformed responses and provider
failures are explicit errors. No audio provider, credential or fallback is
selected implicitly.

The embedding-host consumer contract suite tests real isolated workers against a deterministic authenticated HTTP provider, STT metadata, TTS WAV/voice/speed, ambient credential/proxy canaries and cancellation reaping. This is not a commercial provider or physical microphone qualification.

When a host submits a `TextFinal` with `source="stt"`, the stream keeps the original voice file in the durable user message and attachment links, while passing the transcript as text to the chat model. Other attachments in the same turn still reach the model. A typed turn with untranscribed audio retains native audio capability routing. This lets a host use a text-only chat model with a separate STT provider without falsely claiming the chat model accepts audio.

The public worker protocol regression runs real subprocesses with scripted provider IO and deliberately noisy cached handlers, native fd writes, background threads and atexit callbacks. It parses the complete stdout as one JSON object for success, failure and malformed/oversize input, rather than ignoring extra output. This covers the shutdown isolation boundary, not a commercial STT provider or microphone.

## Speech synthesis as an agent tool

An embedding host can register `synthesize_speech(text, language?)` using
its configured voice and canonical artifact repository.

```python
from openagent_core import SpeechSynthesisCapability

speech = SpeechSynthesisCapability(voice_service, artifact_repository)
catalog.register("speech", speech, speech, target_label="Speech")
```

The catalog authorizes discovery, invocation and publication. The tool
requires the exact active run context and checks the agent and session
artifact-write permissions before invoking `VoiceService`. The artifact
repository rechecks those permissions when it stores the audio. The model
supplies up to 2,000 characters and an optional language tag such as
`it-IT`. Voice, provider, credentials and fallback come from the host.

The call gives synthesis up to 120 seconds and accepts up to 8 MiB of WAV,
MP3, FLAC, Ogg, AAC or MPEG-4 audio, subject to the repository's own size
limit. The tool returns an MCP structured result with a path-free
`structuredContent.artifact` reference. Authorized host code can retrieve
the bytes with `ArtifactRepository.read(context, artifact_id=...)`. The
artifact belongs to the current session. Chat delivery uses its own
authorization and presentation flow. Each invocation requests new synthesis
and a host must handle any operation-level retry policy separately.

The public speech-synthesis tests use a local voice fixture through
`VoiceService`, an admitted runtime turn, and the canonical artifact
repository. They cover exact WAV recovery, cross-tenant denial, write
revocation, malformed input, output bounds, and redacted backend errors.
