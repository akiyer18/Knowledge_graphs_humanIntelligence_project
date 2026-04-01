from __future__ import annotations

from .utils import comparison_form, expand_acronyms, normalize_space, split_identifier


def build_query_terms(label: str, alternatives: list[str] | None = None) -> list[str]:
    values = [label]
    if alternatives:
        values.extend(alternatives)
    query_terms: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        variants = [value, split_identifier(value)]
        variants.extend(expand_acronyms(value))
        for variant in variants:
            normalized = normalize_space(variant)
            if not normalized:
                continue
            key = comparison_form(normalized)
            if key in seen:
                continue
            seen.add(key)
            query_terms.append(normalized)
    return query_terms


def normalize_query(label: str, alternatives: list[str] | None = None) -> tuple[str, str]:
    query_terms = build_query_terms(label, alternatives)
    query = query_terms[0] if query_terms else normalize_space(label)
    return query, comparison_form(query)
