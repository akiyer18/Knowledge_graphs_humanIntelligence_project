from __future__ import annotations

from .normalizer import build_query_terms
from .utils import BaseAPIClient, EntityRecord, ORKG_SOURCE, SearchCandidate


class ORKGClient(BaseAPIClient):
    BASE_URL = "https://orkg.org/api"

    def __init__(self, session, cache, rate_limit_seconds: float) -> None:
        super().__init__(ORKG_SOURCE, session, cache, rate_limit_seconds)

    def search(self, entity: EntityRecord, limit: int) -> list[SearchCandidate]:
        candidates: list[SearchCandidate] = []
        seen: set[tuple[str, str]] = set()
        for query in build_query_terms(entity.local_label, entity.alternative_labels)[:2]:
            for endpoint, params in (
                ("/resources", {"q": query, "size": limit}),
                ("/resources", {"label": query, "size": limit}),
                ("/papers", {"q": query, "size": limit}),
                ("/papers", {"title": query, "size": limit}),
            ):
                try:
                    payload = self.get_json(f"{self.BASE_URL}{endpoint}", params=params)
                except Exception:
                    continue
                for row in self._extract_items(payload):
                    candidate = SearchCandidate(
                        source=ORKG_SOURCE,
                        candidate_uri=row.get("uri") or row.get("id") or "",
                        candidate_id=str(row.get("id") or row.get("identifier") or ""),
                        candidate_label=row.get("label") or row.get("title") or "",
                        candidate_description=row.get("description") or row.get("abstract") or "",
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

    def _extract_items(self, payload):
        if isinstance(payload, list):
            return payload
        if not isinstance(payload, dict):
            return []
        for key in ("content", "results", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return []
