"""Dependency-free lexical retrieval over one authorized discovery snapshot.

Never cache descriptors or references across turns: the caller supplies only
the capabilities admitted for the current principal, audience and generation.
Schema property names are searchable, but schema prose and skill bodies are not
copied into discovery results or used to inflate a tool's relevance.

Terms are lightly stemmed ("schedule", "scheduled" and "schedules" match) and a
tool's document also carries its source id, the source's use-case description
and the source's older names, because a source name often holds the domain a
tool's own description leaves implicit.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
import math
from typing import Any, Optional

from openagent_core.capability_map import STOPWORDS, alias_terms, source_summary, stem, terms as _raw_terms


def _terms(text: str) -> list[str]:
    return [stem(term) for term in _raw_terms(text)]


STOPWORD_STEMS = frozenset(stem(word) for word in STOPWORDS)


def _source_terms(source_id: str, overrides: Optional[Mapping[str, str]] = None) -> list[str]:
    words = _terms(source_id) + _terms(source_summary(source_id, overrides) or "")
    for alias in alias_terms(source_id):
        words += _terms(alias)
    return words


def rank_tools(tools: list[Any], query: str, overrides: Optional[Mapping[str, str]] = None) -> list[Any]:
    """BM25 ranking with exact-name/phrase preference and deterministic ties."""
    query_terms = tuple(term for term in dict.fromkeys(_terms(query)) if term not in STOPWORD_STEMS)
    if query.strip() and not query_terms:
        query_terms = tuple(dict.fromkeys(_terms(query)))
    if query.strip() and not query_terms:
        return []
    documents = []
    frequencies: Counter[str] = Counter()
    source_cache: dict[str, list[str]] = {}
    for tool in tools:
        name = _terms(tool.name)
        properties = tool.input_schema.get("properties", {})
        arguments = " ".join(str(key) for key in properties) if isinstance(properties, Mapping) else ""
        source = source_cache.get(tool.source_id)
        if source is None:
            source = source_cache[tool.source_id] = _source_terms(tool.source_id, overrides)
        counts = Counter(name * 3 + _terms(tool.description or "") + _terms(arguments) + source)
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


def _source_document(entry: Mapping[str, Any]) -> set[str]:
    name = str(entry.get("name") or entry.get("source_ref") or "")
    words = set(_terms(name)) | set(_terms(str(entry.get("target_label") or "")))
    words |= set(_terms(str(entry.get("description") or "")))
    for alias in alias_terms(name):
        words |= set(_terms(alias))
    return words


def filter_sources(sources: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    """Sources whose name, target, description or older names contain every query word.

    Words are stemmed and stopwords ignored, so ``scheduler``, ``cron`` or
    ``scheduled tasks`` find the ``schedules`` source while a precise query
    (a device name and number) still returns exactly that device.
    """
    query_terms = set(_terms(query)) - STOPWORD_STEMS
    if not query_terms:
        return list(sources)
    return [entry for entry in sources if query_terms <= _source_document(entry)]


def rank_sources(tools: list[Any], query: str, overrides: Optional[Mapping[str, str]] = None) -> list[dict[str, Any]]:
    """Distinct sources of ``tools`` ordered by how well they match ``query``."""
    query_terms = set(_terms(query))
    seen: dict[tuple[str, str], dict[str, Any]] = {}
    for tool in tools:
        key = (tool.source_id, tool.target_label)
        if key not in seen:
            seen[key] = {"source_ref": tool.source_id, "target_label": tool.target_label,
                         "description": source_summary(tool.source_id, overrides)}
    scored = []
    for (source_id, _label), entry in seen.items():
        hits = len(query_terms & set(_source_terms(source_id, overrides)))
        if hits:
            scored.append((-hits, source_id, entry))
    scored.sort(key=lambda item: (item[0], item[1]))
    return [entry for _hits, _source, entry in scored]
