"""M1 end to end on the synthetic fixtures: listing -> plan -> fetch -> hours -> report."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from sentinel import m1
from sentinel.config import DateRange, Point, Scope
from sentinel.discovery import LOCATION_COLUMNS
from tests.stubs import StubArchive

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "data/fixtures/synthetic"


@pytest.fixture
def setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Scope, StubArchive, m1.Paths]:
    monkeypatch.chdir(tmp_path)  # provenance and outputs land in tmp, not the repo
    archive = StubArchive()
    for f in sorted(FIX.rglob("*.csv.gz")):
        archive.put(
            f.relative_to(FIX).as_posix(),
            f.read_bytes(),
            modified=datetime(2025, 11, 6, 7, 0, tzinfo=UTC),
        )
    scope = Scope(
        version=1, country_iso="PK",
        lahore=Point(latitude=31.5204, longitude=74.3587, radius_km=25),
        date_range=DateRange(start=date(2025, 11, 1), end=date(2025, 11, 4)),
        parameters=["pm25", "pm10"],
    )  # fmt: skip
    paths = m1.Paths()
    paths.results.mkdir()
    base = dict.fromkeys(LOCATION_COLUMNS, "")
    rows = [
        {**base, "location_id": 99000001, "name": "SYNTHETIC-A", "timezone": "Asia/Karachi",
         "pm_sensors": "pm25:99100011;pm10:99100012", "is_mobile": False, "is_monitor": True,
         "distance_lahore_km": 2.3, "datetime_first_utc": "", "datetime_last_utc": ""},
        {**base, "location_id": 99000002, "name": "SYNTHETIC-B", "timezone": "Asia/Karachi",
         "pm_sensors": "pm25:99100021;pm10:99100022", "is_mobile": False, "is_monitor": False,
         "distance_lahore_km": 30.0, "datetime_first_utc": "", "datetime_last_utc": ""},
    ]  # fmt: skip
    pl.DataFrame(rows).write_csv(paths.locations_csv)
    return scope, archive, paths


def test_pipeline(setup: tuple[Scope, StubArchive, m1.Paths]) -> None:
    scope, archive, paths = setup

    m1.coverage_listing(scope, archive, paths, lahore_only=True)
    listing = pl.read_csv(paths.listing_csv)
    assert listing["location_id"].unique().to_list() == [99000001]  # B is 30 km away
    assert listing.height == 3  # fixtures cover Nov 1-3; Nov 4 has no file

    plan = m1.fetch_plan(paths, [99000001])
    assert plan["files_listed"] == plan["files_to_download"] == 3
    assert archive.downloads == []  # planning downloads nothing

    assert m1.fetch(archive, paths, [99000001]) == 3
    assert m1.fetch_plan(paths, [99000001])["files_to_download"] == 0

    hours = pl.read_csv(m1.coverage_hours(paths))
    assert hours.height == 6 and set(hours["n_hours"]) == {24}

    md = m1.coverage_report(scope, paths).read_text(encoding="utf-8")
    summary = pl.read_csv(paths.summary_csv).to_dicts()[0]
    assert (summary["days_with_file"], summary["days_in_range"]) == (3, 4)
    assert summary["pm25_days_ge18h"] == 3
    assert "| 99000001 | SYNTHETIC-A |" in md and "3/4 (75%)" in md

    prov = (paths.results / "provenance.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(prov) == 4  # listing, fetch, hours, report


def test_snapshot_requires_locations(setup: tuple[Scope, StubArchive, m1.Paths]) -> None:
    scope, archive, paths = setup
    with pytest.raises(SystemExit):
        m1.snapshot(scope, archive, paths)
    scoped = scope.model_copy(update={"snapshot_locations": [99000002]})
    out = m1.snapshot(scoped, archive, paths)
    assert (out / "listing.csv").read_text(encoding="utf-8").count("\n") == 4  # header + 3
