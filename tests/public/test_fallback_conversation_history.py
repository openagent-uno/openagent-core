"""Fallback answers and executed tools survive a real SDK/SQLite next turn."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from openagent_core.core._runner.agent.agent import Agent
from openagent_core.core._runner.team.team import Team
from openagent_core.memory.store.sqlite import SqliteDb
from openagent_core.models.providers.fallback import FallbackConfig, reset_breaker
from openagent_core.models.providers.openai.chat import OpenAIChat


@pytest.mark.asyncio
@pytest.mark.parametrize("team", [False, True])
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("synchronous", [False, True])
async def test_fallback_preserves_history_after_rebuild_and_reopen(tmp_path, team, stream, synchronous):
    requests = []
    answer = "The saved comparison is amber versus violet."
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.headers.get("Authorization") == "Bearer exhausted":
                data = json.dumps({"error": {"message": "rate_limit_error", "type": "rate_limit_error"}}).encode()
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers(); self.wfile.write(data)
                return
            requests.append(payload["messages"])
            if payload["messages"][-1]["role"] == "tool":
                message = {"role": "assistant", "content": answer}
                finish = "stop"
            elif "Remember the comparison" in str(payload["messages"][-1].get("content")):
                message = {"role": "assistant", "content": None, "tool_calls": [{
                    "id": "fixture-call", "type": "function", "function": {"name": "read_note", "arguments": "{}"}}]}
                finish = "tool_calls"
            else:
                message = {"role": "assistant", "content": "I can use the earlier comparison."}
                finish = "stop"
            response = {"id": "fixture", "object": "chat.completion", "created": 1, "model": "fixture",
                        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
                        "usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30}}
            if payload.get("stream"):
                delta = dict(message)
                if delta.get("tool_calls"):
                    delta["tool_calls"] = [{"index": 0, **delta["tool_calls"][0]}]
                response["object"] = "chat.completion.chunk"
                response["choices"] = [{"index": 0, "delta": delta, "finish_reason": None}]
                final = {**response, "choices": [{"index": 0, "delta": {}, "finish_reason": finish}]}
                data = ("data: " + json.dumps(response) + "\n\ndata: " + json.dumps(final) + "\n\ndata: [DONE]\n\n").encode()
            else:
                data = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream" if payload.get("stream") else "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers(); self.wfile.write(data)
    reset_breaker()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    stores = []
    effects = []
    def read_note() -> str:
        """Read a synthetic comparison note."""
        effects.append("read")
        return "fixture-note-719"
    def build(index):
        store = SqliteDb(db_file=str(tmp_path / "session.sqlite")); stores.append(store)
        def model(key):
            return OpenAIChat(id="fixture", api_key=key, base_url=f"http://127.0.0.1:{server.server_port}/v1", max_retries=0)
        primary, backup = model("exhausted"), model("healthy")
        options = dict(id=f"runner-{index}", model=primary, db=store, tools=[read_note],
                       fallback_config=FallbackConfig(on_rate_limit=[backup]),
                       add_history_to_context=True, instructions=f"Synthetic instructions revision {index}")
        return Team(members=[Agent(id="member", model=backup)], **options) if team else Agent(**options)
    try:
        for index, prompt in enumerate(("Remember the comparison", "Compare those two options")):
            runner = build(index)
            if synchronous:
                result = runner.run(prompt, session_id="same-conversation", stream=stream)
                if stream: list(result)
            else:
                result = runner.arun(prompt, session_id="same-conversation", stream=stream)
                if stream:
                    async for _ in result: pass
                else:
                    await result
            stores[-1].close()
        assert effects == ["read"]
        sent = requests[-1]
        assert any(m["role"] == "user" and "Remember the comparison" in str(m.get("content")) for m in sent)
        assert any(m["role"] == "assistant" and m.get("content") == answer for m in sent)
        assert any(m["role"] == "tool" and m.get("content") == "fixture-note-719" for m in sent)
        check = SqliteDb(db_file=str(tmp_path / "session.sqlite")); stores.append(check)
        history = check.get_session("same-conversation").get_messages()
        assert sum(m.role == "assistant" and m.content == answer for m in history) == 1
    finally:
        for store in stores: store.close()
        server.shutdown(); server.server_close(); thread.join(timeout=2)
        reset_breaker()
