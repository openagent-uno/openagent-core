"""Host supplied code execution; the runtime never selects an environment."""
from __future__ import annotations

from typing import Mapping, Protocol
from .contracts import ExecutionContext


class CodeResult(Protocol):
    stdout: str
    stderr: str
    exit_code: int | None
    timed_out: bool
    duration_ms: int


class CodeExecutor(Protocol):
    bridge_transport: str  # "unix" for a shared filesystem, otherwise "files"
    isolated: bool  # host attestation about the provisioned execution boundary
    python_executable: str
    environment: Mapping[str, str]
    workdir: str

    async def prepare(self, context: ExecutionContext) -> None: ...
    async def run(self, *, command: str, cwd: str | None, env: Mapping[str, str],
                  timeout_seconds: float, context: ExecutionContext) -> CodeResult: ...
    async def close(self) -> None: ...


class FileCodeExecutor(CodeExecutor, Protocol):
    """Filesystem bridge for an explicitly provisioned remote environment."""
    async def files_mkdir(self, path: str) -> None: ...
    async def files_listdir(self, path: str) -> list[str]: ...
    async def files_read(self, path: str) -> bytes | None: ...
    async def files_write(self, path: str, data: bytes) -> None: ...
    async def files_rmtree(self, path: str) -> None: ...
