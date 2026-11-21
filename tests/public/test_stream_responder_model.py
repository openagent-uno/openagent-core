"""Real SDK fallback crosses the agent, stream and shared channel collector.

Value: protects=terminal model identifies the responder after fallback;
fails_when=dispatcher returns its initial pick; why_new=provider-only badge
coverage stops before dispatcher; seam=none; no_claim=live provider acceptance
(loopback SSE provider, real SDK and SQLite).
"""
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from openagent_core.core.agent import Agent
from openagent_core.engine import MemoryDB, ModelDispatcher
from openagent_core.models.providers.fallback import reset_breaker
from openagent_core.stream.collector import StreamCollector, fold_outbound_event
from openagent_core.stream.events import TextFinal, now_ms
from openagent_core.stream.session import StreamSession


@pytest.mark.asyncio
async def test_fallback_responder_reaches_the_stream_and_channel_collector(tmp_path):
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            account = self.headers["Authorization"]
            requests.append(account)
            if account == "Bearer primary":
                data = json.dumps({"error": {"message": "rate_limit_error", "type": "rate_limit_error"}}).encode()
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
            else:
                assert payload["stream"]
                chunk = {"id": "fixture", "object": "chat.completion.chunk", "created": 1,
                         "model": "fixture", "choices": [{"index": 0,
                         "delta": {"role": "assistant", "content": "Backup answered."}, "finish_reason": None}]}
                final = {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
                data = ("data: " + json.dumps(chunk) + "\n\ndata: " + json.dumps(final) + "\n\ndata: [DONE]\n\n").encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers(); self.wfile.write(data)
    reset_breaker()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    db = MemoryDB(str(tmp_path / "agent.sqlite")); await db.connect()
    agent = channel_stream = None
    try:
        for name in ("primary", "backup"):
            pid = await db.upsert_provider(name=name, framework="api-based", api_key=name,
                base_url=f"http://127.0.0.1:{server.server_port}/v1")
            await db.upsert_model(provider_id=pid, model="fixture", is_classifier=name == "primary")
        dispatcher = ModelDispatcher(await db.materialise_providers_config(enabled_only=False))
        dispatcher.set_db(db)
        agent = Agent(name="fixture", model=dispatcher, memory=None, system_prompt="Answer the question.")
        channel_stream = StreamSession(agent, client_id="channel", session_id="conversation", coalesce_window_ms=0)
        async def no_audio(_db): return None
        await channel_stream.start(stt_factory=no_audio, tts_factory=no_audio)
        await channel_stream.push_in(TextFinal(session_id="conversation", seq=1, ts_ms=now_ms(), text="Hello"))
        collector = StreamCollector()
        async with asyncio.timeout(20):
            while True:
                event = await channel_stream.outbound.get()
                if fold_outbound_event(collector, event):
                    break
        reply = collector.to_legacy_reply()
        assert reply["type"] == "response", reply
        assert reply["text"] == "Backup answered."
        assert reply["model"] == "backup:fixture"
        assert requests[:2] == ["Bearer primary", "Bearer backup"]
        assert requests.count("Bearer backup") == 1
    finally:
        if channel_stream is not None: await channel_stream.close()
        if agent is not None: await agent.shutdown()
        await db.close()
        server.shutdown(); server.server_close(); thread.join(timeout=2)
        reset_breaker()
