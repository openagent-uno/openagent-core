"""Mandatory, versioned agent prompts and a shared provider cache boundary.

Host blocks are supplied by trusted application assembly, never parsed from a
chat message or an MCP description. They cannot replace framework/module blocks.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from importlib.resources import files
import json
import re
from typing import Any, Iterable, Mapping, Protocol


_DYNAMIC_MARKER = "\n\n<openagent-turn-context>"
_LEGACY_TAIL = re.compile(
    r"\n*(?:<execution-host>[^<]*</execution-host>\s*)?"
    r"(?:<session-id>[^<]*</session-id>\s*)$"
)
_RESERVED = ("<openagent-turn-context>", "</openagent-turn-context>")


@dataclass(frozen=True, slots=True)
class PromptBlock:
    id: str
    revision: str
    text: str
    provenance: str

    def __post_init__(self) -> None:
        if not all(isinstance(v, str) and v.strip() for v in
                   (self.id, self.revision, self.provenance)):
            raise ValueError("prompt block identity, revision and provenance are required")
        if not isinstance(self.text, str) or any(tag in self.text for tag in _RESERVED):
            raise ValueError("prompt text contains a reserved runtime boundary")


@dataclass(frozen=True, slots=True)
class PromptReceipt:
    id: str
    revision: str
    provenance: str
    sha256: str
    dynamic: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "revision": self.revision,
                "provenance": self.provenance, "sha256": self.sha256,
                "dynamic": self.dynamic}


@dataclass(frozen=True, slots=True)
class ComposedPrompt:
    stable: str
    dynamic: str
    receipts: tuple[PromptReceipt, ...]

    @property
    def text(self) -> str:
        return self.stable + self.dynamic

    @property
    def cache_key(self) -> str:
        """Stable provider-prefix key; never use this as a runner/authority key."""
        return sha256(self.stable.encode()).hexdigest()


class HostPromptProvider(Protocol):
    def prompt_blocks(self, context: Any) -> tuple[PromptBlock, ...]: ...


class HostContextProvider(Protocol):
    """Trusted per-turn context prepared by the host before model execution.

    Return JSON-serializable data; never include credentials. Unlike host
    system blocks, this content is kept outside the stable provider prefix.
    """
    def prompt_context(self, context: Any) -> Mapping[str, Any]: ...


def _rules() -> tuple[PromptBlock, ...]:
    return tuple(PromptBlock(**entry) for entry in json.loads(
        files(__package__).joinpath("rules.json").read_text(encoding="utf-8")
    ))


def core_framework_blocks() -> tuple[PromptBlock, ...]:
    """Prompt rules that apply even when the runtime has no optional module."""
    return tuple(block for block in _rules() if block.id == "core.identity")


def rule_blocks(*ids: str) -> tuple[PromptBlock, ...]:
    """Resolve exact versioned rules for an installed module descriptor."""
    requested = tuple(ids)
    available = {block.id: block for block in _rules()}
    missing = [rule_id for rule_id in requested if rule_id not in available]
    if missing:
        raise LookupError("Unknown framework prompt rules: " + ", ".join(missing))
    return tuple(available[rule_id] for rule_id in requested)


def default_framework_text() -> str:
    """Full optional-module baseline for legacy introspection and contract tests."""
    return "\n\n".join(block.text for block in _rules())


def split_prompt(system: str) -> tuple[str, str]:
    """Return stable prefix and dynamic tail for every provider from one contract.

    The legacy tag path reads old prompt snapshots; new prompts use an explicit
    boundary, so capability data, dates of events and session identity cannot
    accidentally enter a shared cache prefix. A delegated role appended after
    the boundary remains in the dynamic tail.
    """
    at = system.find(_DYNAMIC_MARKER)
    if at >= 0:
        return system[:at], system[at:].strip()
    match = _LEGACY_TAIL.search(system)
    return ((system[:match.start()], match.group(0).strip()) if match else (system, ""))


def _substitute(text: str, values: Mapping[str, str]) -> str:
    for key, value in values.items():
        text = text.replace("{{" + key + "}}", value)
    return text


class PromptComposer:
    """Build an agent prompt; callers cannot replace the mandatory baseline."""

    def compose(
        self,
        *,
        framework: Iterable[PromptBlock] | None = None,
        substitutions: Mapping[str, str] | None = None,
        host: Iterable[PromptBlock] = (),
        modules: Iterable[PromptBlock] = (),
        dynamic: Mapping[str, Any] | None = None,
        session_id: str | None = None,
    ) -> ComposedPrompt:
        values = substitutions or {}
        mandatory = core_framework_blocks() if framework is None else tuple(framework)
        module_blocks = tuple(modules)
        host_blocks = tuple(host)
        seen: set[str] = set()
        rendered: list[str] = []
        receipts: list[PromptReceipt] = []
        for block in (*mandatory, *module_blocks, *host_blocks):
            if not isinstance(block, PromptBlock):
                raise TypeError("prompt contributions must be PromptBlock values")
            if block.id in seen:
                raise ValueError(f"duplicate prompt block: {block.id}")
            if block in host_blocks and block.id.startswith(("core.", "module.")):
                raise ValueError("host blocks cannot occupy framework/module identities")
            seen.add(block.id)
            body = _substitute(block.text, values).strip()
            if not body:
                continue
            rendered.append(body)
            receipts.append(PromptReceipt(block.id, block.revision, block.provenance,
                                          sha256(body.encode()).hexdigest()))
        tail = ""
        if dynamic is not None or session_id:
            payload = dict(dynamic or {})
            if session_id:
                payload["session_id"] = session_id
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                 ensure_ascii=False, allow_nan=False).replace("<", "\\u003c")
            tail = _DYNAMIC_MARKER + encoded + "</openagent-turn-context>"
            # Retained as a documented context field for tools and old transcripts.
            if session_id:
                safe_sid = session_id.replace("<", "\\u003c").replace(">", "\\u003e")
                tail += f"\n\n<session-id>{safe_sid}</session-id>"
            receipts.append(PromptReceipt("runtime.turn-context", "1", "runtime",
                                          sha256(encoded.encode()).hexdigest(), True))
        return ComposedPrompt("\n\n".join(rendered), tail, tuple(receipts))


__all__ = ["ComposedPrompt", "HostContextProvider", "HostPromptProvider", "PromptBlock", "PromptComposer",
           "PromptReceipt", "core_framework_blocks", "default_framework_text", "rule_blocks", "split_prompt"]
