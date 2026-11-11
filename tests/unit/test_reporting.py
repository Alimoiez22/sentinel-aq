"""Analysis tables, README injection, Track B change collection, archive source."""

from __future__ import annotations

import csv
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from sentinel.analysis import by_fault_mode, by_mode, inject_readme
from sentinel.config import DateRange, Point, Scope
from sentinel.http import HttpClient
from sentinel.openaq.archive import Archive
from sentinel.production import ArchiveSource, expected_days
from sentinel.rawstore import RawStore
from sentinel.trackb import collect_changes, run_log_window
from tests.conftest import FakeServer, Handler
from tests.stubs import day_key

TABLES = ("fact_measurement_hourly", "mart_daily_location", "mart_exceedance_summary")
DMG = {f"{t}_{k}": 0 for t in TABLES for k in ("missing", "extra", "wrong")}


def _row(
    fault: str, mode: str, correct: bool, esc: bool, exp_esc: bool, **dmg: int
) -> dict[str, object]:
    return {"fault": fault, "mode": mode, "seed": 1, "outputs_correct": correct,
            "handled_as_expected": correct and esc == exp_esc,
            "recovered_verified": correct and not esc,
            "escalated": esc, "escalation_expected": exp_esc, "inappropriate_actions": 0,
            "time_to_verified_recovery_s": 0.5 if correct else None, "overhead_s": 0.1,
            **DMG, **dmg}  # fmt: skip


def test_summaries_count_with_denominators() -> None:
    df = pl.DataFrame([
        _row("F0", "guarded", True, False, False),
        _row("F2", "guarded", True, False, False),
        _row("F2", "retries_only", False, False, False, fact_measurement_hourly_missing=48,
             mart_daily_location_missing=2, mart_exceedance_summary_wrong=2),
        _row("F3", "guarded", True, True, True),
        _row("F3", "retries_only", True, False, True),
    ])  # fmt: skip
    m = {r["mode"]: r for r in by_mode(df).to_dicts()}
    assert m["guarded"]["outputs_correct"] == "2/2"  # F0 excluded from the faulty totals
    assert m["guarded"]["handled_as_expected"] == "2/2"
    assert m["retries_only"]["handled_as_expected"] == "0/2"  # silent block is not "handled"
    assert m["retries_only"]["recovered_verified"] == "1/2"
    assert (m["retries_only"]["wrong_values_published"], m["retries_only"]["missing_location_days"],
            m["retries_only"]["summary_rows_wrong"]) == (0, 1, 2)  # fmt: skip
    fm = by_fault_mode(df)
    assert fm["fault"].to_list()[:2] == ["F0", "F2"]  # numeric fault order, not "F10" < "F2"


def test_readme_block_is_replaced_only_between_markers(tmp_path: Path) -> None:
    readme = tmp_path / "README.md"
    readme.write_text("top\n<!-- results:start -->\nold\n<!-- results:end -->\nbottom\n")
    inject_readme(pl.DataFrame({"mode": ["guarded"], "trials": [1]}), readme)
    text = readme.read_text()
    assert text.startswith("top\n") and text.endswith("bottom\n")
    assert "old" not in text and "| guarded | 1 |" in text
    readme.write_text("no markers")
    with pytest.raises(ValueError):
        inject_readme(pl.DataFrame({"a": [1]}), readme)


def test_trackb_collects_changes_and_counts_value_edits(tmp_path: Path) -> None:
    from tests.integration.conftest import synthetic_files
    from tests.integration.test_pipeline import edit_values

    key, v1 = synthetic_files()[0]
    v2 = edit_values(v1, 2.0, slice(0, 3))
    store = RawStore(tmp_path / "raw")
    s1, _ = store.put(v1)
    s2, _ = store.put(v2)
    snaps = tmp_path / "snaps"
    for run, prev, entries in (
        ("20251106T060000Z", None, [("new", "", s1)]),
        ("20251110T060000Z", "20251106T060000Z", [("changed", s1, s2)]),
    ):
        d = snaps / run
        d.mkdir(parents=True)
        (d / "meta.json").write_text(json.dumps({"run_id": run, "previous_run": prev}))
        with (d / "changes.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "key",
                    "change",
                    "old_etag",
                    "new_etag",
                    "old_sha256",
                    "new_sha256",
                    "old_last_modified_utc",
                    "new_last_modified_utc",
                ]
            )
            for change, old, new in entries:
                w.writerow([key, change, "", "", old, new, "", ""])
    (snaps / "_runs.log").write_text("2025-11-06T06:00:00Z OK\n2025-11-07T06:00:00Z FAILED x\n")
    rows = collect_changes(snaps, store)
    assert len(rows) == 1  # the first snapshot is the baseline inventory, not arrivals
    r = rows[0]
    assert (r["change"], r["values_changed"], r["rows_added"], r["rows_removed"]) == (
        "changed",
        3,
        0,
        0,
    )
    assert r["lateness_h_upper_bound"] == 8 * 24 + 6  # seen Nov 10 06:00, day ended Nov 2 00:00 UTC
    assert run_log_window(snaps) == {
        "attempted": 2,
        "succeeded": 1,
        "failed": 1,
        "first": "2025-11-06T06:00:00Z",
        "last": "2025-11-06T06:00:00Z",
    }


def test_archive_source_maps_missing_to_none(fake_server: Callable[[Handler], FakeServer]) -> None:
    srv = fake_server(lambda p, h: (404, {}, b"") if "missing" in p else (200, {}, b"data"))
    src = ArchiveSource(Archive(HttpClient(sleep=lambda s: None), base_url=srv.url))
    assert src.get("records/missing.csv.gz") is None
    assert src.get(day_key(1, date(2025, 1, 1))) == b"data"


def test_expected_days() -> None:
    scope = Scope(version=1, country_iso="PK", lahore=Point(latitude=31.5, longitude=74.3,
                  radius_km=25), date_range=DateRange(start=date(2025, 1, 1)),
                  parameters=["pm25"], locations=[1, 2])  # fmt: skip
    days = expected_days(scope, date(2025, 1, 1), date(2025, 1, 3))
    assert len(days) == 6 and (2, date(2025, 1, 3)) in days
