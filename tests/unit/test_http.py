"""Bounded retries: transient errors retried, permanent ones not, 429 honours the reset."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from sentinel.http import HttpClient, HttpError
from tests.conftest import FakeServer, Handler


def _client(sleeps: list[float]) -> HttpClient:
    return HttpClient(timeout_s=5, max_attempts=3, backoff_s=1.0, sleep=sleeps.append)


def _sequence(*responses: tuple[int, dict[str, str], bytes]) -> Handler:
    it = iter(responses)
    return lambda path, headers: next(it)


def test_retries_5xx_then_succeeds(fake_server: Callable[[Handler], FakeServer]) -> None:
    srv = fake_server(_sequence((503, {}, b""), (200, {}, b"ok")))
    sleeps: list[float] = []
    assert _client(sleeps).get(srv.url + "/x").body == b"ok"
    assert len(srv.requests) == 2
    assert sleeps == [1.0]


def test_404_is_not_retried(fake_server: Callable[[Handler], FakeServer]) -> None:
    srv = fake_server(lambda p, h: (404, {}, b"missing"))
    sleeps: list[float] = []
    with pytest.raises(HttpError) as exc:
        _client(sleeps).get(srv.url + "/x")
    assert exc.value.status == 404
    assert len(srv.requests) == 1 and sleeps == []


def test_429_waits_for_reset_header(fake_server: Callable[[Handler], FakeServer]) -> None:
    srv = fake_server(_sequence((429, {"x-ratelimit-reset": "7"}, b""), (200, {}, b"ok")))
    sleeps: list[float] = []
    _client(sleeps).get(srv.url + "/x")
    assert sleeps == [8.0]  # reset + 1 s margin


def test_gives_up_after_max_attempts(fake_server: Callable[[Handler], FakeServer]) -> None:
    srv = fake_server(lambda p, h: (500, {}, b"boom"))
    sleeps: list[float] = []
    with pytest.raises(HttpError) as exc:
        _client(sleeps).get(srv.url + "/x")
    assert exc.value.status == 500
    assert len(srv.requests) == 3
    assert sleeps == [1.0, 2.0]  # exponential backoff between the 3 attempts


def test_connection_refused_is_retried_then_raised() -> None:
    sleeps: list[float] = []
    with pytest.raises(OSError):
        _client(sleeps).get("http://127.0.0.1:9/nothing-listens-here")
    assert len(sleeps) == 2
