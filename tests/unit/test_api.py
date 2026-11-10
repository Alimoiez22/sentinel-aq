"""OpenAQ client: key header, paging until a short page, spacing between requests."""

from __future__ import annotations

import json
from collections.abc import Callable
from urllib.parse import parse_qs, urlparse

import pytest

from sentinel.http import HttpClient
from sentinel.openaq import api
from sentinel.openaq.api import OpenAQClient
from tests.conftest import FakeServer, Handler


def test_pages_until_short_page(
    fake_server: Callable[[Handler], FakeServer], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(api, "PAGE_LIMIT", 2)
    pages = {1: [{"id": 1}, {"id": 2}], 2: [{"id": 3}, {"id": 4}], 3: [{"id": 5}]}

    def handler(path: str, headers: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
        q = parse_qs(urlparse(path).query)
        assert urlparse(path).path == "/v3/locations"
        assert q["iso"] == ["PK"] and q["limit"] == ["2"]
        return 200, {}, json.dumps({"results": pages[int(q["page"][0])]}).encode()

    srv = fake_server(handler)
    sleeps: list[float] = []
    clock = iter(float(i) for i in range(100))
    client = OpenAQClient(
        "secret-key", HttpClient(), base_url=srv.url, clock=lambda: next(clock), sleep=sleeps.append
    )
    ids = [loc["id"] for loc in client.iter_locations(iso="PK")]

    assert ids == [1, 2, 3, 4, 5]
    assert len(srv.requests) == 3
    assert all(h["x-api-key"] == "secret-key" for _, h in srv.requests)
    assert sleeps and all(s > 0 for s in sleeps)  # requests were spaced out


def test_exact_multiple_of_page_size_needs_one_empty_page(
    fake_server: Callable[[Handler], FakeServer], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(api, "PAGE_LIMIT", 2)

    def handler(path: str, headers: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
        page = int(parse_qs(urlparse(path).query)["page"][0])
        return (
            200,
            {},
            json.dumps({"results": [{"id": 1}, {"id": 2}] if page == 1 else []}).encode(),
        )

    srv = fake_server(handler)
    client = OpenAQClient("k", HttpClient(), base_url=srv.url, sleep=lambda s: None)
    assert [x["id"] for x in client.iter_locations()] == [1, 2]
    assert len(srv.requests) == 2
