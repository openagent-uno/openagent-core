# Explicit host audio

`openagent_core.audio.VoiceService(db, fallback=None, worker_command=None, timeout_seconds=120)` resolves the latest enabled audio model using the existing provider/model IDs and metadata. `transcribe(path, language=None)` returns text or `None`; `synthesize(text, language=None)` returns `SynthesizedAudio(data, media_type)` or `None`.

Cloud audio uses the optional `audio` extra. Each invocation starts `python -I -m openagent_core.audio_worker`, passing only the operation, configured provider row and input over stdin. The worker gets an explicit minimum environment and temporary HOME, so LiteLLM's SDK globals, credential fallbacks and proxy environment cannot cross host instances. Keys never appear in arguments; stdout contains only a bounded result, stderr is discarded. Cancellation and timeout terminate, then kill if necessary, and await the owned worker. No worker starts at import or on runtime startup.

A frozen product supplies `worker_command=(executable, '_audio-worker')` and dispatches that entrypoint directly to `audio_worker.main()` before normal startup. Local speech models are a host-provisioned `AudioFallback`, with the same two methods. Only an explicitly supplied fallback runs after an empty cloud result, cloud failure or timeout; task cancellation never starts fallback work. Core does not download local models or select an ambient cloud account.

Configured audio requires an explicit key and endpoint (registered OpenAI, Groq and ElevenLabs default endpoints are available); other vendors retain their LiteLLM adapter and require a configured endpoint. TTS sanitization and model metadata remain unchanged. Hosts authorize before invocation and again before publishing results.

The GlassPalace consumer contract suite tests real isolated workers against a deterministic authenticated HTTP provider, STT metadata, TTS WAV/voice/speed, ambient credential/proxy canaries and cancellation reaping. This is not a commercial provider or physical microphone qualification.
