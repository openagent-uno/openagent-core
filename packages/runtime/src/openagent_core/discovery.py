"""Host-supplied presentation for the authorized discovery surface.

These hints never register a source, grant access, or replace an executor.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping


DISCOVERY_PRESENTATION_API_VERSION = 1
_DISCOVERY_TOOLS = frozenset({
    "tool_search_list_servers", "tool_search_list_tools",
    "tool_search_describe_tool", "tool_search_call_tool", "tool_search_call_named_tool",
})


def _text_mapping(values: Mapping[str, str], *, limit: int) -> Mapping[str, str]:
    if not isinstance(values, Mapping):
        raise TypeError("Discovery presentation text must be a mapping")
    copied = {}
    for name, text in values.items():
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Discovery presentation keys must be nonempty strings")
        if not isinstance(text, str) or not text.strip() or len(text) > limit:
            raise ValueError("Discovery presentation text is empty or exceeds its bound")
        copied[name] = text.strip()
    return MappingProxyType(copied)


@dataclass(frozen=True, slots=True)
class DiscoveryPresentation:
    """Optional per-pool presentation, configured by trusted host composition.

    ``capability_sources`` selects exact source IDs whose authorized tools also
    appear as compact name/description hints in the first discovery response.
    The complete inventory still comes from paged ``list_tools``. Descriptions
    affect presentation only; schemas, references and authorization stay intact.
    ``named_tools`` enables a model-facing call by exact source and tool name
    for the supplied pairs. The catalog still resolves and authorizes the call.
    """

    capability_sources: frozenset[str] = frozenset()
    capability_limit: int = 50
    source_summaries: Mapping[str, str] = field(default_factory=dict)
    tool_descriptions: Mapping[str, str] = field(default_factory=dict)
    named_tools: frozenset[tuple[str, str]] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.capability_sources, (set, frozenset, tuple, list)):
            raise TypeError("Capability sources must be a collection of exact IDs")
        sources = frozenset(self.capability_sources)
        if any(not isinstance(name, str) or not name.strip() for name in sources):
            raise ValueError("Capability source IDs must be nonempty strings")
        if type(self.capability_limit) is not int or not 1 <= self.capability_limit <= 50:
            raise ValueError("Capability hint limit must be between 1 and 50")
        summaries = _text_mapping(self.source_summaries, limit=2048)
        descriptions = _text_mapping(self.tool_descriptions, limit=8192)
        if descriptions.keys() - _DISCOVERY_TOOLS:
            raise ValueError("Only discovery tool descriptions may be configured")
        if not isinstance(self.named_tools, (set, frozenset, tuple, list)):
            raise TypeError("Named tools must be a collection of source and tool pairs")
        if len(self.named_tools) > 50 or any(
            not isinstance(pair, tuple) or len(pair) != 2 or any(
                not isinstance(name, str) or not 0 < len(name) <= 256 or name != name.strip()
                for name in pair
            ) for pair in self.named_tools
        ):
            raise ValueError("Named tools require up to 50 exact source and tool pairs")
        object.__setattr__(self, "capability_sources", sources)
        object.__setattr__(self, "source_summaries", summaries)
        object.__setattr__(self, "tool_descriptions", descriptions)
        object.__setattr__(self, "named_tools", frozenset(self.named_tools))
