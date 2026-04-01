from __future__ import annotations

from collections import Counter

from rapidfuzz import fuzz

from .utils import (
    CandidateMatch,
    DBPEDIA_SOURCE,
    EntityRecord,
    HIGH_CONFIDENCE,
    MEDIUM_CONFIDENCE,
    ORKG_SOURCE,
    SearchCandidate,
    WIKIDATA_SOURCE,
    comparison_form,
)


SOURCE_PRIORS = {
    WIKIDATA_SOURCE: 6.0,
    DBPEDIA_SOURCE: 5.0,
    ORKG_SOURCE: 5.0,
}

GENERIC_CANDIDATE_LABELS = {
    "method",
    "concept",
    "model",
    "task",
    "agent",
    "paper",
    "dataset",
}


def score_candidates(
    entities: list[EntityRecord],
    results_by_entity: dict[str, list[SearchCandidate]],
) -> list[CandidateMatch]:
    matches: list[CandidateMatch] = []
    for entity in entities:
        seen: set[tuple[str, str]] = set()
        for candidate in results_by_entity.get(entity.local_uri, []):
            key = candidate.dedupe_key()
            if key in seen:
                continue
            seen.add(key)
            score, reasons = _score(entity, candidate)
            relation = _proposed_relation(entity.inferred_entity_category, score)
            matches.append(
                CandidateMatch(
                    local_uri=entity.local_uri,
                    local_id=entity.local_id,
                    local_label=entity.local_label,
                    inferred_entity_category=entity.inferred_entity_category,
                    normalized_query=entity.normalized_query,
                    source=candidate.source,
                    candidate_uri=candidate.candidate_uri,
                    candidate_label=candidate.candidate_label,
                    candidate_description=candidate.candidate_description,
                    confidence_score=round(max(0.0, min(100.0, score)), 2),
                    proposed_relation=relation,
                    match_reason="; ".join(reasons),
                    candidate_id=candidate.candidate_id,
                )
            )
    matches.sort(key=lambda match: (match.local_uri, -match.confidence_score, match.source))
    return matches


def _score(entity: EntityRecord, candidate: SearchCandidate) -> tuple[float, list[str]]:
    reasons: list[str] = []
    score = SOURCE_PRIORS.get(candidate.source, 0.0)

    local_label = entity.local_label
    candidate_label = candidate.candidate_label or ""
    local_normalized = comparison_form(entity.normalized_query or local_label)
    candidate_normalized = comparison_form(candidate_label)
    alternative_labels = [comparison_form(value) for value in entity.alternative_labels]

    if local_label.lower() == candidate_label.lower():
        score += 35
        reasons.append("exact label match")
    if local_normalized and local_normalized == candidate_normalized:
        score += 25
        reasons.append("normalized exact label match")
    if candidate_normalized in alternative_labels:
        score += 20
        reasons.append("alternative label match")

    fuzzy_score = fuzz.token_set_ratio(local_normalized, candidate_normalized)
    score += fuzzy_score * 0.18
    reasons.append(f"fuzzy similarity {round(fuzzy_score, 1)}")

    description_overlap = _token_overlap(
        entity.description or entity.local_label,
        candidate.candidate_description or candidate_label,
    )
    if description_overlap > 0:
        score += description_overlap * 12
        reasons.append(f"description/token overlap {round(description_overlap, 2)}")

    compatibility = _type_compatibility(entity, candidate)
    if compatibility > 0:
        score += compatibility
        reasons.append("entity category compatible")

    if comparison_form(candidate_label) in GENERIC_CANDIDATE_LABELS:
        score -= 10
        reasons.append("generic candidate penalty")

    if _looks_like_acronym_match(entity, candidate):
        score += 8
        reasons.append("acronym-aware match")

    return score, reasons


def _token_overlap(left: str, right: str) -> float:
    left_tokens = [token for token in comparison_form(left).split() if len(token) > 2]
    right_tokens = [token for token in comparison_form(right).split() if len(token) > 2]
    if not left_tokens or not right_tokens:
        return 0.0
    left_counter = Counter(left_tokens)
    right_counter = Counter(right_tokens)
    intersection = sum((left_counter & right_counter).values())
    union = sum((left_counter | right_counter).values())
    return intersection / union if union else 0.0


def _type_compatibility(entity: EntityRecord, candidate: SearchCandidate) -> float:
    text = " ".join(
        [
            entity.inferred_entity_category,
            candidate.candidate_label,
            candidate.candidate_description,
            " ".join(candidate.aliases),
        ]
    ).lower()
    category = entity.inferred_entity_category
    if category == "Paper" and any(token in text for token in ("paper", "article", "publication")):
        return 12.0
    if category == "Author" and any(token in text for token in ("human", "researcher", "scientist", "author")):
        return 12.0
    if category == "Organization" and any(token in text for token in ("organization", "university", "group", "institute")):
        return 12.0
    if category == "Location" and any(token in text for token in ("city", "country", "place", "region")):
        return 12.0
    if category == "Method" and any(token in text for token in ("method", "algorithm", "model", "approach")):
        return 10.0
    if category in {"Concept", "Domain", "Metric", "Scenario", "Dataset"}:
        return 6.0
    return 0.0


def _looks_like_acronym_match(entity: EntityRecord, candidate: SearchCandidate) -> bool:
    left = entity.local_label
    right = candidate.candidate_label
    if not left or not right:
        return False
    left_compact = left.replace(" ", "")
    right_compact = right.replace(" ", "")
    return left.isupper() and left_compact.lower() in right_compact.lower()


def _proposed_relation(category: str, score: float) -> str:
    same_as_categories = {"Author", "Paper", "Organization", "Location"}
    if category in same_as_categories:
        if score >= 90:
            return "owl:sameAs"
        if score >= HIGH_CONFIDENCE:
            return "rdfs:seeAlso"
        return "rdfs:seeAlso"

    if score >= 90:
        return "skos:exactMatch"
    if score >= MEDIUM_CONFIDENCE:
        return "skos:closeMatch"
    return "rdfs:seeAlso"


def confidence_bucket(score: float) -> str:
    if score >= HIGH_CONFIDENCE:
        return "high"
    if score >= MEDIUM_CONFIDENCE:
        return "medium"
    return "low"
