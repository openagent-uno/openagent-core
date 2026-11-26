"""Disposable stdio MCP whose startup and calls are observable by its parent test."""

import json
from pathlib import Path
import sys
import time

directory = Path(sys.argv[1])
tool_name = sys.argv[3] if len(sys.argv) > 3 else "read_sample"
failed_lists = int(sys.argv[4]) if len(sys.argv) > 4 else 0
with (directory / "starts").open("a") as log:
    log.write("start\n")

for line in sys.stdin:
    request = json.loads(line)
    if "id" not in request:
        continue
    method = request["method"]
    if method in {"initialize", "tools/list"}:
        with (directory / "discovery").open("a") as log:
            log.write(method + "\n")
    if method == "initialize":
        if len(sys.argv) > 2:
            time.sleep(float(sys.argv[2]))
        while not (directory / "ready").exists():
            time.sleep(0.01)
        result = {
            "protocolVersion": request["params"]["protocolVersion"],
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "local-fixture", "version": "1"},
        }
    elif method == "tools/list":
        if failed_lists:
            failed_lists -= 1
            print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "error": {
                "code": -32603, "message": "Discovery unavailable",
            }}), flush=True)
            continue
        result = {
            "tools": [
                {
                    "name": tool_name,
                    "description": "Read a sample",
                    "inputSchema": {"type": "object", "properties": {}},
                }
            ] if tool_name else []
        }
    elif method == "tools/call":
        with (directory / "calls").open("a") as log:
            log.write("call\n")
        result = {"content": [{"type": "text", "text": "local-stdio-answer"}]}
    else:
        result = {}
    print(
        json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}),
        flush=True,
    )
