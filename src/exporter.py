from __future__ import annotations

import json
from pathlib import Path

from rdflib import Graph, URIRef
from rdflib.namespace import OWL, RDF, RDFS, SKOS

from .scorer import confidence_bucket
from .utils import CandidateMatch, EntityRecord, HIGH_CONFIDENCE, dataframe_to_csv, safe_json_dump

ENTITY_COLUMNS = [
    "local_uri",
    "local_id",
    "local_label",
    "alternative_labels",
    "description",
    "rdf_types",
    "inferred_entity_category",
    "normalized_query",
    "source_file",
]

MATCH_COLUMNS = [
    "local_uri",
    "local_id",
    "local_label",
    "inferred_entity_category",
    "normalized_query",
    "source",
    "candidate_uri",
    "candidate_label",
    "candidate_description",
    "confidence_score",
    "proposed_relation",
    "match_reason",
    "accepted_suggestion",
    "candidate_id",
    "confidence_bucket",
]


def export_all(output_dir: Path, entities: list[EntityRecord], matches: list[CandidateMatch]) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)

    entity_rows = [_entity_row(entity) for entity in entities]
    dataframe_to_csv(entity_rows, output_dir / "extracted_entities.csv", columns=ENTITY_COLUMNS)
    safe_json_dump(output_dir / "extracted_entities.json", entity_rows)

    match_rows = [_match_row(match) for match in matches]
    dataframe_to_csv(match_rows, output_dir / "candidate_matches.csv", columns=MATCH_COLUMNS)
    safe_json_dump(output_dir / "candidate_matches.json", match_rows)

    high_rows = [row for row in match_rows if row["confidence_bucket"] == "high"]
    medium_rows = [row for row in match_rows if row["confidence_bucket"] == "medium"]
    low_rows = [row for row in match_rows if row["confidence_bucket"] == "low"]
    dataframe_to_csv(high_rows, output_dir / "review_high_confidence.csv", columns=MATCH_COLUMNS)
    dataframe_to_csv(medium_rows, output_dir / "review_medium_confidence.csv", columns=MATCH_COLUMNS)
    dataframe_to_csv(low_rows, output_dir / "review_low_confidence.csv", columns=MATCH_COLUMNS)

    ttl_links = write_proposed_links(output_dir / "proposed_links.ttl", matches)
    return {
        "high": len(high_rows),
        "medium": len(medium_rows),
        "low": len(low_rows),
        "ttl_links": ttl_links,
    }


def _entity_row(entity: EntityRecord) -> dict[str, object]:
    return {
        "local_uri": entity.local_uri,
        "local_id": entity.local_id,
        "local_label": entity.local_label,
        "alternative_labels": json.dumps(entity.alternative_labels, ensure_ascii=False),
        "description": entity.description,
        "rdf_types": json.dumps(entity.rdf_types, ensure_ascii=False),
        "inferred_entity_category": entity.inferred_entity_category,
        "normalized_query": entity.normalized_query,
        "source_file": entity.source_file,
    }


def _match_row(match: CandidateMatch) -> dict[str, object]:
    return {
        "local_uri": match.local_uri,
        "local_id": match.local_id,
        "local_label": match.local_label,
        "inferred_entity_category": match.inferred_entity_category,
        "normalized_query": match.normalized_query,
        "source": match.source,
        "candidate_uri": match.candidate_uri,
        "candidate_label": match.candidate_label,
        "candidate_description": match.candidate_description,
        "confidence_score": match.confidence_score,
        "proposed_relation": match.proposed_relation,
        "match_reason": match.match_reason,
        "accepted_suggestion": match.accepted_suggestion,
        "candidate_id": match.candidate_id,
        "confidence_bucket": confidence_bucket(match.confidence_score),
    }


def write_proposed_links(path: Path, matches: list[CandidateMatch]) -> int:
    graph = Graph()
    graph.bind("owl", OWL)
    graph.bind("skos", SKOS)
    graph.bind("rdfs", RDFS)

    count = 0
    seen: set[tuple[str, str, str]] = set()
    for match in matches:
        if match.confidence_score < HIGH_CONFIDENCE:
            continue
        if match.local_uri.startswith("_:"):
            continue
        triple_key = (match.local_uri, match.proposed_relation, match.candidate_uri)
        if triple_key in seen:
            continue
        seen.add(triple_key)
        predicate = {
            "owl:sameAs": OWL.sameAs,
            "skos:exactMatch": SKOS.exactMatch,
            "skos:closeMatch": SKOS.closeMatch,
            "rdfs:seeAlso": RDFS.seeAlso,
        }[match.proposed_relation]
        graph.add((URIRef(match.local_uri), predicate, URIRef(match.candidate_uri)))
        count += 1

    graph.serialize(destination=path, format="turtle")
    return count
