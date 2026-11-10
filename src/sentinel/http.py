"""Minimal HTTP GET with bounded retries (standard library only).

Why not `requests`/`httpx`: we only need GET, and fewer dependencies means fewer
versions to pin. Transport is injectable, so tests use an in-process fake server.

Retry policy (bounded, rule from the SRE literature in docs/RESEARCH.md §7.7):
only transient failures are retried (connection errors, timeouts, 5xx, 429), at most
`max_attempts` times, with exponential backoff. 4xx other than 429 is never retried:
it will not fix itself.
"""

from __future__ import annotations

import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from sentinel.log import get_logger

USER_AGENT = "sentinel-aq/0.1 (research pipeline)"


@dataclass(frozen=True)
class Response:
    """Status, lower-cased headers and body of one HTTP response."""

    status: int
    headers: Mapping[str, str]
    body: bytes


class HttpError(RuntimeError):
    """A response that is not 2xx after all allowed attempts."""

    def __init__(self, url: str, status: int, body: bytes) -> None:
        super().__init__(f"HTTP {status} for {url}: {body[:200]!r}")
        self.url, self.status, self.body = url, status, body


@dataclass
class HttpClient:
    """GET with bounded retries. `sleep` is injectable so tests do not wait."""

    timeout_s: float = 60.0
    max_attempts: int = 5
    backoff_s: float = 2.0
    sleep: Callable[[float], None] = time.sleep
    default_headers: dict[str, str] = field(default_factory=lambda: {"User-Agent": USER_AGENT})

    def _once(self, url: str, headers: Mapping[str, str]) -> Response:
        req = urllib.request.Request(url, headers={**self.default_headers, **headers})  # noqa: S310 - https URLs built by us
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:  # noqa: S310
                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                return Response(resp.status, hdrs, resp.read())
        except urllib.error.HTTPError as err:
            hdrs = {k.lower(): v for k, v in err.headers.items()} if err.headers else {}
            return Response(err.code, hdrs, err.read())

    def get(self, url: str, headers: Mapping[str, str] | None = None) -> Response:
        """GET `url`. Retries transient failures; raises HttpError or the last OSError."""
        log = get_logger(url=url)
        last_exc: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self._once(url, headers or {})
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_exc, resp = exc, None
            if resp is not None and 200 <= resp.status < 300:
                return resp
            if resp is not None and resp.status != 429 and resp.status < 500:
                raise HttpError(url, resp.status, resp.body)  # permanent: do not retry
            if attempt == self.max_attempts:
                break
            wait = self._wait_seconds(resp, attempt)
            log.warning(
                "http_retry",
                attempt=attempt,
                status=getattr(resp, "status", None),
                error=repr(last_exc) if resp is None else None,
                wait_s=wait,
            )
            self.sleep(wait)
        if resp is not None:
            raise HttpError(url, resp.status, resp.body)
        if last_exc is None:  # pragma: no cover - loop always sets one of the two
            raise RuntimeError(f"no response and no error for {url}")
        raise last_exc

    def _wait_seconds(self, resp: Response | None, attempt: int) -> float:
        """429 with a numeric reset header: wait that long (capped). Else exponential."""
        if resp is not None and resp.status == 429:
            reset = resp.headers.get("x-ratelimit-reset") or resp.headers.get("retry-after")
            if reset is not None and reset.strip().isdigit() and int(reset) <= 3600:
                return float(int(reset) + 1)
            return 60.0
        return float(self.backoff_s * 2 ** (attempt - 1))
