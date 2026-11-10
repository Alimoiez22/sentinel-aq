"""Smoke test against the real OpenAQ bucket. Run with `make test-network`.

Excluded from `make test` and CI: results depend on a third party and the network.
Uses location 2178 (the docs' example), January 2024, read once on 2026-11-05.
"""

from __future__ import annotations

from datetime import date

import pytest

from sentinel.coverage import detect_header, file_hours
from sentinel.http import HttpClient
from sentinel.openaq.archive import Archive

pytestmark = pytest.mark.network


def test_list_and_read_one_real_month() -> None:
    archive = Archive(HttpClient(timeout_s=60))
    objs = archive.list_location(2178, date(2024, 1, 1), date(2024, 1, 31))
    assert len(objs) == 31  # observed on 2026-11-05; a failure here is a finding
    first = objs[0]
    data = archive.download(first.key)
    assert len(data) == first.size
    assert detect_header(data).variant in {"A", "B"}
    rows = file_hours(data, first.key)
    assert rows and all(int(str(r["n_hours"])) > 0 for r in rows)
