"""Dependency-free lexical retrieval over one authorized discovery snapshot.

Never cache descriptors or references across turns: the caller supplies only
the capabilities admitted for the current principal, audience and generation.
Schema property names are searchable, but schema prose and skill bodies are not
copied into discovery results or used to inflate a tool's relevance.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
import math
import re
from typing import Any


def _terms(text: str) -> list[str]:
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(text))
    return re.findall(r"[^\W_]+", text.casefold())


def rank_tools(tools: list[Any], query: str) -> list[Any]:
    """BM25 ranking with exact-name/phrase preference and deterministic ties."""
    query_terms = tuple(dict.fromkeys(_terms(query)))
    if query.strip() and not query_terms:
        return []
    documents = []
    frequencies: Counter[str] = Counter()
    for tool in tools:
        name = _terms(tool.name)
        properties = tool.input_schema.get("properties", {})
        arguments = " ".join(str(key) for key in properties) if isinstance(properties, Mapping) else ""
        counts = Counter(name * 3 + _terms(tool.description or "") + _terms(arguments))
        documents.append((tool, name, counts, sum(counts.values())))
        frequencies.update(counts.keys())
    average_length = sum(item[3] for item in documents) / max(1, len(documents)) or 1
    phrase = " ".join(query_terms)
    ranked = []
    for tool, name, counts, length in documents:
        score = 0.0
        for term in query_terms:
            frequency = counts[term]
            if frequency:
                idf = math.log1p((len(documents) - frequencies[term] + 0.5) / (frequencies[term] + 0.5))
                score += idf * frequency * 2.2 / (frequency + 1.2 * (0.25 + 0.75 * length / average_length))
        if query_terms and not score:
            continue
        normalized_name = " ".join(name)
        preference = (2 if normalized_name == phrase else
                      1 if phrase and f" {phrase} " in f" {normalized_name} " else 0)
        ranked.append((preference, score, tool))
    ranked.sort(key=lambda item: (-item[0], -item[1], item[2].name.casefold(),
                                  item[2].name, item[2].source_id, item[2].target_label))
    return [tool for _preference, _score, tool in ranked]
