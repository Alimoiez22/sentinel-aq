"""Location flattening, distance, scope validation and provenance records."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from sentinel.config import Scope, load_scope
from sentinel.discovery import LOCATION_COLUMNS, flatten_location, haversine_km
from sentinel.provenance import git_commit, record

ROOT = Path(__file__).resolve().parents[2]

# Shaped like the example payload of docs.openaq.org/resources/locations (checked 2026-11-11).
LOC = {
    "id": 7, "name": "Site", "locality": "Lahore", "timezone": "Asia/Karachi",
    "country": {"id": 1, "code": "PK", "name": "Pakistan"},
    "owner": {"id": 2, "name": "Owner"}, "provider": {"id": 3, "name": "Provider"},
    "isMobile": False, "isMonitor": True, "instruments": [{"id": 4, "name": "Inst"}],
    "sensors": [
        {"id": 11, "name": "pm25 µg/m³", "parameter": {"id": 2, "name": "pm25", "units": "µg/m³"}},
        {"id": 12, "name": "o3 ppm", "parameter": {"id": 3, "name": "o3", "units": "ppm"}},
    ],
    "coordinates": {"latitude": 31.5204, "longitude": 74.3587},
    "licenses": [{"id": 9, "name": "CC BY 4.0"}],
    "datetimeFirst": {"utc": "2024-01-01T00:00:00Z", "local": "x"},
    "datetimeLast": {"utc": "2026-11-01T00:00:00Z", "local": "x"},
}  # fmt: skip


def test_flatten_location() -> None:
    row = flatten_location(LOC, 31.5204, 74.3587, ("pm25", "pm10"))
    assert list(row) == LOCATION_COLUMNS
    assert row["pm_sensors"] == "pm25:11"
    assert row["parameters"] == "o3;pm25"
    assert row["distance_lahore_km"] == 0.0
    assert (row["provider"], row["is_monitor"], row["licenses"]) == ("Provider", True, "CC BY 4.0")


def test_missing_fields_stay_empty() -> None:
    row = flatten_location({"id": 8}, 31.5, 74.3, ("pm25",))
    assert row["distance_lahore_km"] is None
    assert row["pm_sensors"] == "" and row["provider"] == "" and row["is_monitor"] is None


def test_one_degree_of_latitude() -> None:
    assert haversine_km(0, 0, 1, 0) == pytest.approx(111.195, abs=0.001)


def test_repo_scope_file_is_valid() -> None:
    scope = load_scope(ROOT / "config/scope.yaml")
    assert scope.country_iso == "PK" and scope.parameters == ["pm25", "pm10"]
    assert scope.date_range.resolved_end(today=date(2026, 11, 11)) == date(2026, 11, 7)


def _scope_dict() -> dict[str, object]:
    return {
        "version": 1,
        "country_iso": "PK",
        "lahore": {"latitude": 31.5, "longitude": 74.3, "radius_km": 25},
        "date_range": {"start": "2024-01-01", "end": None},
        "parameters": ["pm25"],
    }


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(typo_key=1),
        lambda d: d.update(date_range={"start": "2025-01-02", "end": "2025-01-01"}),
        lambda d: d.update(lahore={"latitude": 31.5, "longitude": 74.3, "radius_km": 30}),
        lambda d: d.update(country_iso="pak"),
    ],
)
def test_invalid_scopes_fail(mutate: Callable[[dict[str, object]], object]) -> None:
    d = _scope_dict()
    mutate(d)
    with pytest.raises(ValidationError):
        Scope.model_validate(d)


def test_git_commit_from_ref_and_packed_refs(tmp_path: Path) -> None:
    git = tmp_path / ".git"
    (git / "refs/heads").mkdir(parents=True)
    (git / "HEAD").write_text("ref: refs/heads/main\n")
    (git / "refs/heads/main").write_text("abc123\n")
    assert git_commit(tmp_path) == "abc123"
    (git / "refs/heads/main").unlink()
    (git / "packed-refs").write_text("# pack\ndef456 refs/heads/main\n")
    assert git_commit(tmp_path) == "def456"
    assert git_commit(tmp_path / "nowhere") == "unknown"


def test_record_appends_json_lines(tmp_path: Path) -> None:
    log = tmp_path / "prov.jsonl"
    record("cmd-a", ["out1"], {"n": 1}, log_file=log)
    record("cmd-b", ["out2"], log_file=log)
    lines = [json.loads(x) for x in log.read_text(encoding="utf-8").splitlines()]
    assert [x["command"] for x in lines] == ["cmd-a", "cmd-b"]
    assert lines[0]["n"] == 1 and lines[0]["working_tree_clean"] == "not checked"
