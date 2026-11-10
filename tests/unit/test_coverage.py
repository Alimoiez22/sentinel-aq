"""Hours per day from files, header-variant detection, arrival lag, summary denominators."""

from __future__ import annotations

import gzip
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from sentinel.coverage import detect_header, file_hours, listing_rows, summarise
from sentinel.openaq.archive import ObjectInfo
from tests.stubs import day_key

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "data/fixtures/synthetic/records/csv.gz"
FILE_A = FIX / "locationid=99000001/year=2025/month=11/location-99000001-20251101.csv.gz"
FILE_B = FIX / "locationid=99000002/year=2025/month=11/location-99000002-20251101.csv.gz"


def _key(path: Path) -> str:
    return path.relative_to(ROOT / "data/fixtures/synthetic").as_posix()


def test_both_known_variants_give_24_hours() -> None:
    for path, variant in ((FILE_A, "A"), (FILE_B, "B")):
        rows = file_hours(path.read_bytes(), _key(path))
        assert {r["parameter"] for r in rows} == {"pm10", "pm25"}
        for r in rows:
            assert (r["header_variant"], r["n_hours"], r["n_sensors"], r["n_rows"]) == (
                variant, 24, 1, 24
            )  # fmt: skip
            assert r["n_bad_datetime"] == 0


def test_unknown_header_is_reported_not_guessed() -> None:
    raw = gzip.compress(b"location_id,sensor,when,value\n1,2,2025-11-01T01:00:00+05:00,3\n")
    key = day_key(1, date(2025, 11, 1))
    assert detect_header(raw).variant == "unknown"
    rows = file_hours(raw, key)
    assert len(rows) == 1
    assert rows[0]["header_variant"] == "unknown"
    assert rows[0]["header"] == "location_id,sensor,when,value"
    assert rows[0]["n_hours"] == 0


def test_bad_and_duplicate_timestamps() -> None:
    header = "location_id,sensors_id,location,datetime,lat,lon,parameter,units,value\n"
    body = (
        "1,9,X,2025-11-01T01:00:00+05:00,0,0,pm25,µg/m³,10\n"
        "1,9,X,2025-11-01T01:00:00+05:00,0,0,pm25,µg/m³,10\n"  # duplicate hour
        "1,9,X,not-a-time,0,0,pm25,µg/m³,10\n"
        "1,8,X,2025-11-01T02:00:00+05:00,0,0,pm25,µg/m³,12\n"  # second sensor
    )
    raw = gzip.compress((header + body).encode())
    (row,) = file_hours(raw, day_key(1, date(2025, 11, 1)))
    assert (row["n_hours"], row["n_rows"], row["n_bad_datetime"], row["n_sensors"]) == (2, 4, 1, 2)


def test_arrival_lag_uses_location_timezone() -> None:
    key = day_key(3, date(2025, 11, 1))
    # Local day ends 2025-11-02 00:00 +05:00 = 2025-11-01 19:00 UTC; written 84 h later.
    obj = ObjectInfo(key, 10, "e", datetime(2025, 11, 5, 7, 0, tzinfo=UTC))
    (row,) = listing_rows(3, [obj], "Asia/Karachi")
    assert row["arrival_lag_h"] == 84.0
    (row_no_tz,) = listing_rows(3, [obj], "")
    assert row_no_tz["arrival_lag_h"] is None


def test_summary_denominators_and_thresholds() -> None:
    start, end = date(2025, 10, 30), date(2025, 11, 2)  # 4 days, 2 of them in winter (Nov)
    locations = pl.DataFrame({"location_id": [1, 2], "distance_lahore_km": [3.0, 1.0]})
    listing = pl.DataFrame({
        "location_id": [1, 1, 1], "source_date": ["2025-10-30", "2025-11-01", "2025-11-02"],
        "arrival_lag_h": [80.0, 84.0, 90.0], "size_bytes": [10, 10, 10],
    })  # fmt: skip
    hours = pl.DataFrame({
        "location_id": [1, 1, 1], "source_date": ["2025-10-30", "2025-11-01", "2025-11-02"],
        "parameter": ["pm25", "pm25", "pm25"], "n_hours": [24, 18, 17],
    })  # fmt: skip
    s = summarise(locations, listing, hours, start, end, ["pm25"])
    r1 = s.filter(pl.col("location_id") == 1).to_dicts()[0]
    assert (r1["days_in_range"], r1["days_with_file"]) == (4, 3)
    assert (r1["winter_days_in_range"], r1["winter_days_with_file"]) == (2, 2)
    assert (r1["pm25_days_ge18h"], r1["pm25_winter_days_ge18h"]) == (2, 1)  # 17 h fails
    assert r1["median_arrival_lag_h"] == 84.0
    r2 = s.filter(pl.col("location_id") == 2).to_dicts()[0]
    assert r2["days_with_file"] == 0  # listed nothing: an explicit zero, not missing
    assert s["location_id"].to_list() == [2, 1]  # nearest to Lahore first
