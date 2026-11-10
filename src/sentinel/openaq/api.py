"""OpenAQ v3 REST client, only what M1 needs: list locations.

Facts used (checked 2026-11-11 against https://api.openaq.org/openapi.json):
key header `X-API-Key`; `/v3/locations` accepts `iso`, `coordinates`, `radius`
(<= 25000 m), `limit`, `page`. Free tier: 60 requests/minute, 2,000/hour, 429 when
exceeded (docs: using-the-api/rate-limits). We stay below the limit by spacing requests,
because repeated 429s can get a key banned.
"""

from __future__ import annotations

import time
import urllib.parse
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

from sentinel.http import HttpClient

API_URL = "https://api.openaq.org"
PAGE_LIMIT = 1000


@dataclass
class OpenAQClient:
    """Paged GETs against the v3 API with client-side spacing (>= 1.1 s apart)."""

    api_key: str
    http: HttpClient
    base_url: str = API_URL
    min_interval_s: float = 1.1
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    _last: float = field(default=-1e9, init=False)

    def get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """One GET returning the decoded JSON body."""
        import json

        wait = self._last + self.min_interval_s - self.clock()
        if wait > 0:
            self.sleep(wait)
        url = f"{self.base_url}{path}?{urllib.parse.urlencode(params)}"
        resp = self.http.get(url, headers={"X-API-Key": self.api_key, "Accept": "application/json"})
        self._last = self.clock()
        body: dict[str, Any] = json.loads(resp.body)
        return body

    def iter_locations(self, **params: Any) -> Iterator[dict[str, Any]]:
        """All locations matching `params`, page by page until a short page."""
        page = 1
        while True:
            body = self.get_json("/v3/locations", {**params, "limit": PAGE_LIMIT, "page": page})
            results: list[dict[str, Any]] = body.get("results", [])
            yield from results
            if len(results) < PAGE_LIMIT:
                return
            page += 1
