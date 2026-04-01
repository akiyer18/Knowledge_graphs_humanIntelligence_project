from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOGGER_NAME = "ontology_linker"

WIKIDATA_SOURCE = "Wikidata"
DBPEDIA_SOURCE = "DBpedia"
ORKG_SOURCE = "ORKG"
DEFAULT_SOURCES = [WIKIDATA_SOURCE, DBPEDIA_SOURCE, ORKG_SOURCE]

HIGH_CONFIDENCE = 82.0
MEDIUM_CONFIDENCE = 60.0


@dataclass
class EntityRecord:
    local_uri: str
    local_id: str
    local_label: str
    alternative_labels: list[str] = field(default_factory=list)
    description: str = ""
    rdf_types: list[str] = field(default_factory=list)
    inferred_entity_category: str = "Unknown"
    normalized_query: str = ""
    source_file: str = ""
    comparison_label: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SearchCandidate:
    source: str
    candidate_uri: str
    candidate_id: str
    candidate_label: str
    candidate_description: str = ""
    aliases: list[str] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def dedupe_key(self) -> tuple[str, str]:
        return (self.source, self.candidate_uri or self.candidate_id)


@dataclass
class CandidateMatch:
    local_uri: str
    local_id: str
    local_label: str
    inferred_entity_category: str
    normalized_query: str
    source: str
    candidate_uri: str
    candidate_label: str
    candidate_description: str
    confidence_score: float
    proposed_relation: str
    match_reason: str
    accepted_suggestion: bool = False
    candidate_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineSummary:
    extracted_entities: int = 0
    queried_terms: int = 0
    candidates_per_source: dict[str, int] = field(default_factory=dict)
    high_confidence_matches: int = 0
    medium_confidence_matches: int = 0
    low_confidence_matches: int = 0
    proposed_ttl_links: int = 0


@dataclass
class PipelineConfig:
    input_path: Path
    output_dir: Path
    cache_dir: Path
    sources: list[str]
    max_queries: int | None
    max_candidates_per_source: int
    timeout: int
    rate_limit_seconds: float
    log_level: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extract ontology entities, search external KGs, and export candidate links."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input ontology Turtle file.",
    )
    parser.add_argument(
        "--output_dir",
        default="outputs",
        help="Directory where review artifacts will be written.",
    )
    parser.add_argument(
        "--sources",
        nargs="*",
        choices=DEFAULT_SOURCES,
        default=DEFAULT_SOURCES,
        help="External sources to query. Default: all.",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=None,
        help="Limit the number of extracted entities to query for testing.",
    )
    parser.add_argument(
        "--max-candidates-per-source",
        type=int,
        default=5,
        help="Maximum candidates to retain per source for each local entity.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--rate-limit-seconds",
        type=float,
        default=0.25,
        help="Delay between external requests.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level.",
    )
    return parser


def parse_args(argv: list[str] | None = None) -> PipelineConfig:
    parser = build_parser()
    args = parser.parse_args(argv)
    output_dir = Path(args.output_dir).resolve()
    cache_dir = output_dir / "cache"
    return PipelineConfig(
        input_path=Path(args.input).resolve(),
        output_dir=output_dir,
        cache_dir=cache_dir,
        sources=args.sources,
        max_queries=args.max_queries,
        max_candidates_per_source=args.max_candidates_per_source,
        timeout=args.timeout,
        rate_limit_seconds=args.rate_limit_seconds,
        log_level=args.log_level,
    )


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def ensure_directories(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def build_session(timeout: int) -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "ontology-candidate-linker/1.0 (+local-review-pipeline)",
        }
    )
    session.request = _with_timeout(session.request, timeout)
    return session


def _with_timeout(request_fn: Any, timeout: int) -> Any:
    def wrapped(method: str, url: str, **kwargs: Any) -> requests.Response:
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return request_fn(method, url, **kwargs)

    return wrapped


class HTTPCache:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        ensure_directories(cache_dir)

    def _path(self, namespace: str, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.cache_dir / namespace / f"{digest}.json"

    def get(self, namespace: str, key: str) -> Any | None:
        path = self._path(namespace, key)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def set(self, namespace: str, key: str, value: Any) -> None:
        path = self._path(namespace, key)
        ensure_directories(path.parent)
        path.write_text(json.dumps(value, indent=2, ensure_ascii=False), encoding="utf-8")


class BaseAPIClient:
    def __init__(
        self,
        source_name: str,
        session: requests.Session,
        cache: HTTPCache,
        rate_limit_seconds: float,
    ) -> None:
        self.source_name = source_name
        self.session = session
        self.cache = cache
        self.rate_limit_seconds = rate_limit_seconds
        self.logger = logging.getLogger(f"{LOGGER_NAME}.{source_name.lower()}")

    def sleep(self) -> None:
        if self.rate_limit_seconds > 0:
            time.sleep(self.rate_limit_seconds)

    def get_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        cache_key: str | None = None,
        timeout: int | None = None,
    ) -> Any:
        stable_key = json.dumps({"url": url, "params": params or {}}, sort_keys=True)
        key = cache_key or stable_key
        cached = self.cache.get(self.source_name.lower(), key)
        if cached is not None:
            return cached
        self.sleep()
        request_kwargs: dict[str, Any] = {"params": params}
        if timeout is not None:
            request_kwargs["timeout"] = timeout
        response = self.session.get(url, **request_kwargs)
        response.raise_for_status()
        payload = response.json()
        self.cache.set(self.source_name.lower(), key, payload)
        return payload


def safe_json_dump(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def dataframe_to_csv(records: Iterable[dict[str, Any]], path: Path, columns: list[str] | None = None) -> None:
    frame = pd.DataFrame(list(records), columns=columns)
    frame.to_csv(path, index=False)


def normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def split_identifier(value: str) -> str:
    value = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", value)
    value = value.replace("_", " ")
    value = value.replace("-", " ")
    value = normalize_space(value)
    return value


def comparison_form(value: str) -> str:
    return normalize_space(re.sub(r"[^a-z0-9\s]", " ", value.lower()))


ACRONYM_EXPANSIONS = {
    "xai": "Explainable Artificial Intelligence",
    "hitl": "Human In The Loop",
    "hi": "Hybrid Intelligence",
    "kg": "Knowledge Graph",
    "llm": "Large Language Model",
    "nlp": "Natural Language Processing",
    "vr": "Virtual Reality",
    "eeg": "Electroencephalography",
    "abss": "Agent Based Social Simulation",
}


def expand_acronyms(value: str) -> list[str]:
    tokens = comparison_form(value).split()
    expansions: list[str] = []
    for token in tokens:
        expansion = ACRONYM_EXPANSIONS.get(token)
        if expansion:
            expansions.append(expansion)
    return expansions


def make_local_id(uri: str) -> str:
    if uri.startswith("_:"):
        return uri[2:]
    if "#" in uri:
        return uri.rsplit("#", 1)[-1]
    return uri.rstrip("/").rsplit("/", 1)[-1]


def list_to_json(value: list[str]) -> str:
    return json.dumps(value, ensure_ascii=False)


def parse_listish_value(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return [str(value)]


def main(argv: list[str] | None = None) -> int:
    from .dbpedia_client import DBpediaClient
    from .exporter import export_all
    from .extractor import extract_entities
    from .orkg_client import ORKGClient
    from .parser import parse_ontology
    from .scorer import score_candidates
    from .wikidata_client import WikidataClient

    config = parse_args(argv)
    configure_logging(config.log_level)
    logger = logging.getLogger(LOGGER_NAME)
    ensure_directories(config.output_dir, config.cache_dir)

    graph = parse_ontology(config.input_path)
    entities = extract_entities(graph=graph, source_file=config.input_path)
    summary = PipelineSummary(extracted_entities=len(entities))

    query_entities = entities[: config.max_queries] if config.max_queries else entities
    summary.queried_terms = len(query_entities)

    session = build_session(config.timeout)
    cache = HTTPCache(config.cache_dir)
    clients = []
    if WIKIDATA_SOURCE in config.sources:
        clients.append(WikidataClient(session, cache, config.rate_limit_seconds))
    if DBPEDIA_SOURCE in config.sources:
        clients.append(DBpediaClient(session, cache, config.rate_limit_seconds))
    if ORKG_SOURCE in config.sources:
        clients.append(ORKGClient(session, cache, config.rate_limit_seconds))

    results_by_entity: dict[str, list[SearchCandidate]] = {entity.local_uri: [] for entity in query_entities}
    if clients:
        for entity in query_entities:
            logger.info("Querying external KGs for %s", entity.local_label)
            for client in clients:
                try:
                    candidates = client.search(entity, config.max_candidates_per_source)
                except Exception as exc:  # pragma: no cover - defensive path for unstable APIs
                    logger.warning("Source %s failed for %s: %s", client.source_name, entity.local_label, exc)
                    candidates = []
                summary.candidates_per_source[client.source_name] = (
                    summary.candidates_per_source.get(client.source_name, 0) + len(candidates)
                )
                results_by_entity[entity.local_uri].extend(candidates)

    matches = score_candidates(query_entities, results_by_entity)
    export_summary = export_all(config.output_dir, entities, matches)
    summary.high_confidence_matches = export_summary["high"]
    summary.medium_confidence_matches = export_summary["medium"]
    summary.low_confidence_matches = export_summary["low"]
    summary.proposed_ttl_links = export_summary["ttl_links"]

    print(f"Extracted ontology entities: {summary.extracted_entities}")
    print(f"Queried terms: {summary.queried_terms}")
    for source_name in DEFAULT_SOURCES:
        print(f"Candidates found in {source_name}: {summary.candidates_per_source.get(source_name, 0)}")
    print(f"High confidence matches: {summary.high_confidence_matches}")
    print(f"Medium confidence matches: {summary.medium_confidence_matches}")
    print(f"Low confidence matches: {summary.low_confidence_matches}")
    print(f"Proposed TTL links generated: {summary.proposed_ttl_links}")
    return 0
