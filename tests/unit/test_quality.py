"""Quality rules on small, hand-made day frames (no database)."""

from __future__ import annotations

import gzip
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from sentinel.contract import parse_file
from sentinel.quality import DayContext, evaluate, load_quality_config

ROOT = Path(__file__).resolve().parents[2]
CFG = load_quality_config(ROOT / "config/quality_rules.yaml")
DAY = date(2025, 11, 1)
CTX = DayContext(1, DAY, "Asia/Karachi", ("pm25", "pm10"), datetime(2026, 1, 1, tzinfo=UTC),
                 (31.5, 74.3))  # fmt: skip
HEADER = "location_id,sensors_id,location,datetime,lat,lon,parameter,units,value\n"


def day(values: list[str], offset: str = "+05:00", unit: str = "µg/m³") -> bytes:
    """One sensor, 24 hourly rows (01:00 .. 24:00 local) with the given values."""
    rows = []
    for h, v in enumerate(values, start=1):
        ts = datetime(2025, 11, 1) + timedelta(hours=h)
        rows.append(f"1,9,X,{ts:%Y-%m-%dT%H:%M:%S}{offset},31.5,74.3,pm25,{unit},{v}")
    return gzip.compress((HEADER + "\n".join(rows) + "\n").encode())


def run(data: bytes):  # type: ignore[no-untyped-def]
    return evaluate(parse_file(data).frame, CFG, CTX)


def test_clean_day_is_accepted() -> None:
    r = run(day([f"{50 + i}.0" for i in range(24)]))
    assert (r.accepted.height, r.rejects.height, r.blocked) == (24, 0, False)


def test_missing_value_code_is_missing_not_bad() -> None:
    """Q00 (ADR-021): 8 of 24 hours = -999 -> quarantined as Q00, NOT blocked."""
    vals = [f"{50 + i}.0" for i in range(24)]
    for i in range(8):
        vals[i] = "-999.0"
    r = run(day(vals))
    assert r.counts["Q00_missing_code"] == 8
    assert "Q03_negative" not in r.counts
    assert (r.accepted.height, r.blocked, r.reject_rate) == (16, False, 0.0)


def test_real_negatives_still_block_when_too_many() -> None:
    vals = [f"{50 + i}.0" for i in range(24)]
    for i in range(8):
        vals[i] = "-3.0"  # 8/24 = 33 % > 20 %
    r = run(day(vals))
    assert r.counts["Q03_negative"] == 8 and r.blocked


def test_codes_and_bad_rows_mixed() -> None:
    """Rate = bad rows / (rows - missing codes): 4 bad of 16 measured = 25 % -> block."""
    vals = [f"{50 + i}.0" for i in range(24)]
    for i in range(8):
        vals[i] = "-999.0"
    for i in range(8, 12):
        vals[i] = "#ERR"
    r = run(day(vals))
    assert (r.counts["Q00_missing_code"], r.counts["Q02_bad_value"]) == (8, 4)
    assert r.reject_rate == 0.25 and r.blocked


def test_offset_unit_ceiling_and_flags() -> None:
    assert run(day(["10.0"] * 24, offset="+00:00")).counts["Q08_offset_mismatch"] == 24
    assert run(day(["10.0"] * 24, unit="ppm")).unknown_units == ["ppm"]
    vals = [f"{50 + i}.0" for i in range(24)]
    vals[5], vals[6] = "1500.0", "700.0"
    r = run(day(vals))
    assert r.counts["Q04_above_ceiling"] == 1 and r.counts["Q12_high_band"] == 1
    stuck = run(day(["80.0"] * 24))
    assert stuck.counts["Q13_stuck"] == 24  # flagged, kept
    assert stuck.accepted.height == 24


def test_mg_per_m3_is_converted_exactly() -> None:
    r = run(day(["0.05"] * 24, unit="mg/m³"))
    assert r.accepted["value"].to_list() == [50.0] * 24
