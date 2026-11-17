"""Discovery from the provenance CSV and the real-data experiment window."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from sentinel import m1
from sentinel.config import DateRange, Point, Scope
from sentinel.discovery import LOCATION_COLUMNS
from sentinel.experiments import real_dataset
from sentinel.rawstore import write_atomic
from tests.stubs import day_key

ROOT = Path(__file__).resolve().parents[2]
SCOPE = Scope(
    version=1,
    country_iso="PK",
    lahore=Point(latitude=31.5204, longitude=74.3587, radius_km=25),
    date_range=DateRange(start=date(2019, 5, 1)),
    parameters=["pm25", "pm10"],
)


def test_repo_location_csv_has_provenance_and_valid_rows() -> None:
    df = pl.read_csv(ROOT / "config/locations_lahore.csv", infer_schema_length=None)
    assert set(LOCATION_COLUMNS) - {"distance_lahore_km"} <= set(df.columns)
    assert df["source"].null_count() == 0  # every row says where it came from
    assert df["timezone"].to_list() == ["Asia/Karachi"] * df.height


def test_discover_from_csv_computes_distance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    paths = m1.Paths()
    out = m1.discover_from_csv(SCOPE, ROOT / "config/locations_lahore.csv", paths)
    df = pl.read_csv(out)
    assert df.columns == LOCATION_COLUMNS
    assert df.sort("location_id")["distance_lahore_km"].to_list() == [4.91, 5.36]
    assert (paths.results / "provenance.jsonl").read_text().count("discover-from-csv") == 1


def test_real_window_reads_landing_and_places_run0(tmp_path: Path) -> None:
    for d in (date(2024, 11, 14), date(2024, 11, 15)):
        write_atomic(tmp_path / day_key(8664, d), f"bytes-{d}".encode())
    days: list[dict[str, object]] = [
        {"location_id": 8664, "start": "2024-11-14", "end": "2024-11-15"}
    ]
    window: dict[str, object] = {"landing": str(tmp_path), "days": days}
    clean, run0 = real_dataset(window)
    assert len(clean) == 2
    assert run0 == datetime(2024, 11, 19, 8, 0, tzinfo=UTC)  # Nov 16 00:00 UTC + 80 h
    days[0]["end"] = "2024-11-16"  # a day that was never fetched
    with pytest.raises(FileNotFoundError, match="make fetch"):
        real_dataset(window)
