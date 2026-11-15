"""Mandatory, versioned agent prompts and a shared provider cache boundary.

Host blocks are supplied by trusted application assembly, never parsed from a
chat message or an MCP description. They cannot replace framework/module blocks.

Framework and module blocks are ordered by purpose, not by module id: each block
may name a ``section`` from :data:`SECTION_ORDER` and a ``priority`` inside it.
The composer sorts them by ``(section, priority, id)``, so the order is
deterministic and the stable provider-cache prefix does not depend on module
start order. Blocks without a known section keep their input order after the
sectioned ones; host blocks always follow, in the order the host supplied.
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

# Purpose order of framework and module blocks. Host blocks come after all of
# them. Append new sections deliberately: the order is part of the cache prefix.
SECTION_ORDER: tuple[str, ...] = (
    "identity", "principles", "tools", "routing", "memory", "history",
    "automation", "delegation", "files", "skills", "communication",
)
_SECTION_RANK = {name: rank for rank, name in enumerate(SECTION_ORDER)}


@dataclass(frozen=True, slots=True)
class PromptBlock:
    id: str
    revision: str
    text: str
    provenance: str
    # Optional purpose placement (see SECTION_ORDER). ``None`` or an unknown
    # section keeps the block's input position after the sectioned blocks.
    section: str | None = None
    priority: int = 0

    def __post_init__(self) -> None:
        if not all(isinstance(v, str) and v.strip() for v in
                   (self.id, self.revision, self.provenance)):
            raise ValueError("prompt block identity, revision and provenance are required")
        if not isinstance(self.text, str) or any(tag in self.text for tag in _RESERVED):
            raise ValueError("prompt text contains a reserved runtime boundary")
        if self.section is not None and (not isinstance(self.section, str) or not self.section.strip()):
            raise ValueError("prompt block section must be a non-empty string or None")
        if type(self.priority) is not int:
            raise TypeError("prompt block priority must be an integer")


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


def order_blocks(blocks: Iterable[PromptBlock]) -> tuple[PromptBlock, ...]:
    """Sort framework/module blocks by ``(section, priority, id)``.

    Blocks with no section, or a section this runtime does not know, keep their
    relative input order and follow every sectioned block. The sort is total
    and deterministic, so equal inputs always compose to the same prefix.
    """
    indexed = []
    for index, block in enumerate(blocks):
        if not isinstance(block, PromptBlock):
            raise TypeError("prompt contributions must be PromptBlock values")
        rank = _SECTION_RANK.get(block.section) if block.section is not None else None
        key = ((len(SECTION_ORDER), 0, "", index) if rank is None
               else (rank, block.priority, block.id, index))
        indexed.append((key, block))
    return tuple(block for _key, block in sorted(indexed, key=lambda item: item[0]))


# Framework rules every run receives, whichever modules are installed. Every
# other rule in rules.json is selected by a module descriptor.
_ALWAYS_ON = frozenset({"core.identity", "core.principles", "core.reporting"})


def core_framework_blocks() -> tuple[PromptBlock, ...]:
    """Prompt rules that apply even when the runtime has no optional module."""
    return order_blocks(block for block in _rules() if block.id in _ALWAYS_ON)


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
    return "\n\n".join(block.text for block in order_blocks(_rules()))


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
        # Framework and module blocks are ordered by purpose; host blocks keep
        # the host's order and always follow them.
        ordered = order_blocks((*mandatory, *tuple(modules)))
        host_blocks = tuple(host)
        seen: set[str] = set()
        rendered: list[str] = []
        receipts: list[PromptReceipt] = []
        for block in (*ordered, *host_blocks):
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
           "PromptReceipt", "SECTION_ORDER", "core_framework_blocks", "default_framework_text",
           "order_blocks", "rule_blocks", "split_prompt"]
