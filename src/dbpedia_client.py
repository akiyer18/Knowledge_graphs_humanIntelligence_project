from __future__ import annotations

import requests

from .normalizer import build_query_terms
from .utils import BaseAPIClient, DBPEDIA_SOURCE, EntityRecord, SearchCandidate


class DBpediaClient(BaseAPIClient):
    ENDPOINT = "https://dbpedia.org/sparql"
    QUERY_TIMEOUT_SECONDS = 8

    def __init__(self, session, cache, rate_limit_seconds: float) -> None:
        super().__init__(DBPEDIA_SOURCE, session, cache, rate_limit_seconds)

    def search(self, entity: EntityRecord, limit: int) -> list[SearchCandidate]:
        candidates: list[SearchCandidate] = []
        seen: set[tuple[str, str]] = set()
        for query in build_query_terms(entity.local_label, entity.alternative_labels)[:2]:
            sparql = self._build_query(query, limit)
            try:
                payload = self.get_json(
                    self.ENDPOINT,
                    params={
                        "query": sparql,
                        "format": "application/sparql-results+json",
                        "timeout": str(self.QUERY_TIMEOUT_SECONDS * 1000),
                    },
                    timeout=self.QUERY_TIMEOUT_SECONDS,
                )
            except requests.exceptions.Timeout:
                self.logger.warning("DBpedia timed out for query '%s'; skipping", query)
                continue
            except requests.exceptions.RequestException as exc:
                self.logger.warning("DBpedia request failed for query '%s': %s", query, exc)
                continue
            rows = payload.get("results", {}).get("bindings", [])
            for row in rows:
                candidate = SearchCandidate(
                    source=DBPEDIA_SOURCE,
                    candidate_uri=row.get("resource", {}).get("value", ""),
                    candidate_id=row.get("resource", {}).get("value", "").rsplit("/", 1)[-1],
                    candidate_label=row.get("label", {}).get("value", ""),
                    candidate_description=row.get("abstract", {}).get("value", ""),
                    raw_payload=row,
                )
                key = candidate.dedupe_key()
                if not candidate.candidate_uri or key in seen:
                    continue
                seen.add(key)
                candidates.append(candidate)
                if len(candidates) >= limit:
                    return candidates
        return candidates

    def _build_query(self, label: str, limit: int) -> str:
        safe_label = label.replace("\\", "\\\\").replace('"', '\\"')
        return f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbo: <http://dbpedia.org/ontology/>
SELECT DISTINCT ?resource ?label ?abstract WHERE {{
  ?resource rdfs:label ?label .
  FILTER (langMatches(lang(?label), "en"))
  OPTIONAL {{
    ?resource dbo:abstract ?abstract .
    FILTER (langMatches(lang(?abstract), "en"))
  }}
  FILTER (
    STRSTARTS(lcase(str(?label)), lcase("{safe_label}")) ||
    lcase(str(?label)) = lcase("{safe_label}")
  )
}}
ORDER BY DESC(IF(lcase(str(?label)) = lcase("{safe_label}"), 1, 0)) ASC(STRLEN(STR(?label)))
LIMIT {limit}
""".strip()
