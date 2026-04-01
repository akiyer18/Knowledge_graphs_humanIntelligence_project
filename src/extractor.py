from __future__ import annotations

import logging
from pathlib import Path

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import DCTERMS, FOAF, OWL, RDF, RDFS, SKOS

from .normalizer import normalize_query
from .utils import EntityRecord, make_local_id, normalize_space, split_identifier


logger = logging.getLogger("ontology_linker.extractor")

SCHEMA_NAME = URIRef("http://schema.org/name")
SCHEMA_DESCRIPTION = URIRef("http://schema.org/description")
DC_TITLE = URIRef("http://purl.org/dc/elements/1.1/title")
DC_DESCRIPTION = URIRef("http://purl.org/dc/elements/1.1/description")
TEXTUAL_LABEL_PROPERTIES = {
    RDFS.label,
    SKOS.prefLabel,
    SKOS.altLabel,
    FOAF.name,
    DCTERMS.title,
    DCTERMS.alternative,
    DCTERMS.identifier,
    SCHEMA_NAME,
    DC_TITLE,
}
TEXTUAL_DESCRIPTION_PROPERTIES = {
    RDFS.comment,
    DCTERMS.description,
    DCTERMS.abstract,
    SCHEMA_DESCRIPTION,
    DC_DESCRIPTION,
}
LOCAL_TYPE_HINTS = {
    "method": "Method",
    "model": "Method",
    "algorithm": "Method",
    "paper": "Paper",
    "article": "Paper",
    "publication": "Paper",
    "author": "Author",
    "person": "Author",
    "researcher": "Author",
    "organization": "Organization",
    "university": "Organization",
    "group": "Organization",
    "location": "Location",
    "city": "Location",
    "country": "Location",
    "scenario": "Scenario",
    "use case": "Scenario",
    "context": "Scenario",
    "domain": "Domain",
    "metric": "Metric",
    "dataset": "Dataset",
    "benchmark": "Dataset",
    "concept": "Concept",
    "task": "Concept",
    "capability": "Concept",
}


def extract_entities(graph: Graph, source_file: Path) -> list[EntityRecord]:
    entities: dict[str, EntityRecord] = {}
    for subject in sorted(set(graph.subjects()), key=str):
        if not isinstance(subject, (URIRef, BNode)):
            continue
        if not _is_local_resource(subject):
            continue
        entity = _build_entity_record(graph, subject, source_file)
        if entity is None:
            continue
        entities[entity.local_uri] = entity
    logger.info("Extracted %s local ontology entities", len(entities))
    return list(entities.values())


def _build_entity_record(graph: Graph, subject: URIRef | BNode, source_file: Path) -> EntityRecord | None:
    labels = _collect_literals(graph, subject, TEXTUAL_LABEL_PROPERTIES)
    descriptions = _collect_literals(graph, subject, TEXTUAL_DESCRIPTION_PROPERTIES)
    rdf_types = [str(obj) for obj in graph.objects(subject, RDF.type)]

    if isinstance(subject, BNode) and not labels:
        return None

    local_uri = str(subject) if isinstance(subject, URIRef) else f"_:{subject}"
    local_id = make_local_id(local_uri)

    if not labels and not rdf_types:
        return None

    primary_label = labels[0] if labels else split_identifier(local_id)
    alternative_labels = [label for label in labels[1:] if label != primary_label]
    description = descriptions[0] if descriptions else ""
    inferred_category = _infer_entity_category(primary_label, description, rdf_types)
    normalized_query, comparison_label = normalize_query(primary_label, alternative_labels)

    return EntityRecord(
        local_uri=local_uri,
        local_id=local_id,
        local_label=primary_label,
        alternative_labels=sorted(set(alternative_labels)),
        description=description,
        rdf_types=sorted(set(rdf_types)),
        inferred_entity_category=inferred_category,
        normalized_query=normalized_query,
        source_file=str(source_file),
        comparison_label=comparison_label,
    )


def _collect_literals(graph: Graph, subject: URIRef | BNode, predicates: set[URIRef]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for predicate in predicates:
        for obj in graph.objects(subject, predicate):
            if isinstance(obj, Literal):
                value = normalize_space(str(obj))
                if value and value not in seen:
                    seen.add(value)
                    values.append(value)
    return values


def _is_local_resource(resource: URIRef | BNode) -> bool:
    if isinstance(resource, BNode):
        return True
    resource_str = str(resource)
    for prefix in (
        str(RDF),
        str(RDFS),
        str(OWL),
        str(SKOS),
        str(FOAF),
        str(DCTERMS),
        "http://schema.org/",
        "http://purl.org/dc/elements/1.1/",
        "http://www.w3.org/2001/XMLSchema#",
    ):
        if resource_str.startswith(prefix):
            return False
    return True


def _infer_entity_category(label: str, description: str, rdf_types: list[str]) -> str:
    text = " ".join([label, description] + rdf_types).lower()
    if any(token in text for token in ("paper", "article", "publication", "preprint", "conference")):
        return "Paper"
    if any(token in text for token in ("author", "person", "scientist", "researcher", "clinician", "teacher")):
        return "Author"
    if any(token in text for token in ("organization", "university", "group", "lab", "institute")):
        return "Organization"
    if any(token in text for token in ("city", "country", "location", "place", "region")):
        return "Location"
    if any(token in text for token in ("scenario", "use case", "interaction", "context")):
        return "Scenario"
    if "domain" in text:
        return "Domain"
    if any(token in text for token in ("metric", "score", "accuracy", "evaluation")):
        return "Metric"
    if any(token in text for token in ("dataset", "corpus", "benchmark")):
        return "Dataset"
    if any(token in text for token in ("method", "model", "algorithm", "reasoning")):
        return "Method"
    for hint, category in LOCAL_TYPE_HINTS.items():
        if hint in text:
            return category
    return "Concept"
