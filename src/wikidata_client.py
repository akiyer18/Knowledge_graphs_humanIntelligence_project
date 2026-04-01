from __future__ import annotations

from .normalizer import build_query_terms
from .utils import BaseAPIClient, EntityRecord, SearchCandidate, WIKIDATA_SOURCE


class WikidataClient(BaseAPIClient):
    SEARCH_URL = "https://www.wikidata.org/w/api.php"

    def __init__(self, session, cache, rate_limit_seconds: float) -> None:
        super().__init__(WIKIDATA_SOURCE, session, cache, rate_limit_seconds)

    def search(self, entity: EntityRecord, limit: int) -> list[SearchCandidate]:
        candidates: list[SearchCandidate] = []
        seen: set[tuple[str, str]] = set()
        for query in build_query_terms(entity.local_label, entity.alternative_labels)[:3]:
            payload = self.get_json(
                self.SEARCH_URL,
                params={
                    "action": "wbsearchentities",
                    "format": "json",
                    "language": "en",
                    "uselang": "en",
                    "type": "item",
                    "limit": limit,
                    "search": query,
                },
            )
            for row in payload.get("search", []):
                candidate = SearchCandidate(
                    source=WIKIDATA_SOURCE,
                    candidate_uri=f"https://www.wikidata.org/entity/{row.get('id', '')}",
                    candidate_id=row.get("id", ""),
                    candidate_label=row.get("label", ""),
                    candidate_description=row.get("description", ""),
                    aliases=row.get("aliases", []) or [],
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
