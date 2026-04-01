from __future__ import annotations

import logging
from pathlib import Path

from rdflib import Graph


logger = logging.getLogger("ontology_linker.parser")


def parse_ontology(input_path: Path) -> Graph:
    graph = Graph()
    logger.info("Parsing ontology from %s", input_path)
    graph.parse(input_path, format="turtle")
    logger.info("Parsed %s triples", len(graph))
    return graph
