"""M1 commands: discover, coverage-listing, fetch, coverage-hours, coverage-report, snapshot.

Each command reads and writes files under data/ (never committed) and results/
(committed evidence), and appends a provenance line to results/provenance.jsonl.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from sentinel.config import Scope
from sentinel.coverage import file_hours, listing_rows, summarise
from sentinel.discovery import LOCATION_COLUMNS, flatten_location
from sentinel.fetch import Fetcher, FetchIndex
from sentinel.http import HttpClient
from sentinel.log import get_logger
from sentinel.openaq.api import OpenAQClient
from sentinel.openaq.archive import Archive, ObjectInfo, months
from sentinel.provenance import record
from sentinel.rawstore import RawStore
from sentinel.snapshot import Snapshotter


@dataclass(frozen=True)
class Paths:
    """Where M1 reads and writes. Relative to the repository root."""

    data: Path = Path("data")
    results: Path = Path("results")

    @property
    def locations_csv(self) -> Path:
        return self.results / "m1_locations.csv"

    @property
    def listing_csv(self) -> Path:
        return self.results / "m1_coverage_listing.csv"

    @property
    def hours_csv(self) -> Path:
        return self.results / "m1_coverage_hours.csv"

    @property
    def summary_csv(self) -> Path:
        return self.results / "m1_coverage_summary.csv"

    @property
    def summary_md(self) -> Path:
        return self.results / "m1_coverage_summary.md"


def make_fetcher(archive: Archive, paths: Paths) -> Fetcher:
    """Fetcher wired to the standard data folders."""
    return Fetcher(
        archive=archive,
        store=RawStore(paths.data / "raw"),
        landing_root=paths.data / "landing_drop",
        index=FetchIndex(paths.data / "raw" / "index.csv"),
    )


def discover(scope: Scope, client: OpenAQClient, paths: Paths) -> Path:
    """All locations of the scope country -> raw JSON + flat CSV."""
    raw = list(client.iter_locations(iso=scope.country_iso))
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    raw_path = paths.data / "metadata" / f"locations_{scope.country_iso}_{stamp}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(raw, indent=1, sort_keys=True), encoding="utf-8")
    rows = [
        flatten_location(r, scope.lahore.latitude, scope.lahore.longitude,
                         tuple(scope.parameters))
        for r in raw
    ]  # fmt: skip
    df = (
        pl.DataFrame(
            rows,
            schema_overrides={"is_monitor": pl.Boolean, "is_mobile": pl.Boolean},
            infer_schema_length=None,
        )
        .select(LOCATION_COLUMNS)
        .sort("location_id")
    )
    paths.results.mkdir(parents=True, exist_ok=True)
    df.write_csv(paths.locations_csv)
    record(
        "discover",
        [str(paths.locations_csv), str(raw_path)],
        {"country_iso": scope.country_iso, "n_locations": df.height},
    )
    return paths.locations_csv


def candidates(paths: Paths, scope: Scope, lahore_only: bool) -> pl.DataFrame:
    """Fixed-site locations with at least one sensor for a scope parameter."""
    df = pl.read_csv(paths.locations_csv, infer_schema_length=None)
    df = df.filter(pl.col("pm_sensors").is_not_null() & (pl.col("is_mobile") != True))  # noqa: E712
    if lahore_only:
        df = df.filter(pl.col("distance_lahore_km") <= scope.lahore.radius_km)
    return df


def _active_months(
    first_utc: str | None, last_utc: str | None, start: date, end: date
) -> tuple[date, date] | None:
    """Clip [start, end] to the location's active span, when the API reports one.

    The API gives UTC instants; local dates can differ by one day, so the span is
    widened by a day on each side rather than risk missing a file.
    """
    one = timedelta(days=1)
    lo = max(start, date.fromisoformat(first_utc[:10]) - one) if first_utc else start
    hi = min(end, date.fromisoformat(last_utc[:10]) + one) if last_utc else end
    return (lo, hi) if lo <= hi else None


def coverage_listing(scope: Scope, archive: Archive, paths: Paths, lahore_only: bool) -> Path:
    """S3 listings for every candidate -> one row per day file."""
    start, end = scope.date_range.start, scope.date_range.resolved_end()
    log = get_logger(step="coverage_listing")
    rows: list[dict[str, object]] = []
    n_requests = 0
    for c in candidates(paths, scope, lahore_only).iter_rows(named=True):
        span = _active_months(c["datetime_first_utc"], c["datetime_last_utc"], start, end)
        if span is None:
            continue
        objs = archive.list_location(c["location_id"], *span)
        n_requests += sum(1 for _ in months(*span))
        rows.extend(listing_rows(c["location_id"], objs, c["timezone"] or ""))
        log.info("listed", location_id=c["location_id"], files=len(objs))
    schema = {"location_id": pl.Int64, "source_date": pl.String, "key": pl.String,
              "size_bytes": pl.Int64, "etag": pl.String, "last_modified_utc": pl.String,
              "arrival_lag_h": pl.Float64}  # fmt: skip
    pl.DataFrame(rows, schema=schema).sort(["location_id", "source_date"]).write_csv(
        paths.listing_csv
    )
    record("coverage-listing", [str(paths.listing_csv)],
           {"lahore_only": lahore_only, "date_start": str(start), "date_end": str(end),
            "n_list_requests_at_least": n_requests, "n_files": len(rows)})  # fmt: skip
    return paths.listing_csv


def listed_objects(paths: Paths, location_ids: list[int]) -> list[ObjectInfo]:
    """ObjectInfo for the chosen locations, from the coverage listing."""
    df = pl.read_csv(paths.listing_csv, infer_schema_length=None).filter(
        pl.col("location_id").is_in(location_ids)
    )
    return [
        ObjectInfo(r["key"], r["size_bytes"], r["etag"],
                   datetime.fromisoformat(r["last_modified_utc"]))
        for r in df.iter_rows(named=True)
    ]  # fmt: skip


def fetch_plan(paths: Paths, location_ids: list[int]) -> dict[str, int]:
    """How much `fetch` would download (no network)."""
    objs = listed_objects(paths, location_ids)
    known = FetchIndex(paths.data / "raw" / "index.csv").load()
    todo = [o for o in objs if (o.key, o.etag) not in known]
    return {
        "files_listed": len(objs),
        "files_to_download": len(todo),
        "bytes_to_download": sum(o.size for o in todo),
    }


def fetch(archive: Archive, paths: Paths, location_ids: list[int]) -> int:
    """Download every listed file of the chosen locations not stored yet."""
    objs = listed_objects(paths, location_ids)
    stored = make_fetcher(archive, paths).fetch(objs)
    record(
        "fetch",
        [str(paths.data / "raw" / "index.csv")],
        {"location_ids": sorted(location_ids), "n_files": len(stored)},
    )
    return len(stored)


def coverage_hours(paths: Paths) -> Path:
    """Hours per parameter per day for every listed file that has been fetched."""
    listing = pl.read_csv(paths.listing_csv, infer_schema_length=None)
    known = FetchIndex(paths.data / "raw" / "index.csv").load()
    store = RawStore(paths.data / "raw")
    rows: list[dict[str, object]] = []
    for r in listing.iter_rows(named=True):
        sha = known.get((r["key"], r["etag"]))
        if sha is not None:
            rows.extend(file_hours(store.get(sha), r["key"]))
    schema = {"location_id": pl.Int64, "source_date": pl.String, "key": pl.String,
              "header_variant": pl.String, "parameter": pl.String, "n_sensors": pl.Int64,
              "n_hours": pl.Int64, "n_rows": pl.Int64, "n_bad_datetime": pl.Int64,
              "header": pl.String}  # fmt: skip
    pl.DataFrame(rows, schema=schema).sort(["location_id", "source_date", "parameter"]).write_csv(
        paths.hours_csv
    )
    record("coverage-hours", [str(paths.hours_csv)], {"n_rows": len(rows)})
    return paths.hours_csv


def coverage_report(scope: Scope, paths: Paths) -> Path:
    """Summary CSV + Markdown table generated from the result files (rule 4)."""
    start, end = scope.date_range.start, scope.date_range.resolved_end()
    locations = pl.read_csv(paths.locations_csv, infer_schema_length=None)
    listing = pl.read_csv(paths.listing_csv, infer_schema_length=None)
    hours = (
        pl.read_csv(paths.hours_csv, infer_schema_length=None) if paths.hours_csv.exists() else None
    )
    listed_ids = listing["location_id"].unique().to_list()
    summary = summarise(
        locations.filter(pl.col("location_id").is_in(listed_ids)),
        listing,
        hours,
        start,
        end,
        scope.parameters,
    )
    summary.write_csv(paths.summary_csv)
    paths.summary_md.write_text(render_markdown(summary, start, end, scope), encoding="utf-8")
    record("coverage-report", [str(paths.summary_csv), str(paths.summary_md)])
    return paths.summary_md


def _pct(n: object, d: object) -> str:
    if not isinstance(n, int) or not isinstance(d, int) or d == 0:
        return "not measured"
    return f"{n}/{d} ({100 * n / d:.0f}%)"


def render_markdown(summary: pl.DataFrame, start: date, end: date, scope: Scope) -> str:
    """Markdown table; every number is a column of results/m1_coverage_summary.csv."""
    lines = [
        "# M1 coverage summary (generated; do not edit by hand)",
        "",
        f"Source dates {start} to {end}. Generated from results/m1_coverage_*.csv by "
        "`make coverage-report`. 'not measured' = files not fetched yet.",
        "Winter = Nov-Feb. A day counts as >=18h when at least 18 distinct hourly timestamps "
        "exist for the parameter (EU 2008/50/EC criterion, RESEARCH §2.9).",
        "",
        "| id | name | provider | monitor | km to Lahore | days with file | winter days with file "
        + "".join(f"| {p} days >=18h | {p} winter >=18h " for p in scope.parameters)
        + "| median arrival lag h | licences |",
        "|" + "---|" * (9 + 2 * len(scope.parameters)),
    ]
    for r in summary.iter_rows(named=True):
        cells = [
            str(r["location_id"]),
            str(r["name"]),
            str(r["provider"]),
            str(r["is_monitor"]),
            str(r["distance_lahore_km"]),
            _pct(r["days_with_file"], r["days_in_range"]),
            _pct(r["winter_days_with_file"], r["winter_days_in_range"]),
        ]
        for p in scope.parameters:
            cells.append(_pct(r.get(f"{p}_days_ge18h"), r["days_in_range"]))
            cells.append(_pct(r.get(f"{p}_winter_days_ge18h"), r["winter_days_in_range"]))
        lag = r["median_arrival_lag_h"]
        cells += [
            f"{lag:.1f}" if isinstance(lag, float) else "not measured",
            str(r["licenses"] or ""),
        ]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def snapshot(scope: Scope, archive: Archive, paths: Paths) -> Path:
    """Track B daily snapshot of scope.snapshot_locations."""
    if not scope.snapshot_locations:
        raise SystemExit("config/scope.yaml: snapshot_locations is empty (fill it after discover)")
    start, end = scope.date_range.start, scope.date_range.resolved_end()
    out = Snapshotter(archive, make_fetcher(archive, paths), paths.data / "snapshots").run(
        scope.snapshot_locations, start, end
    )
    record("snapshot", [str(out)], json.loads((out / "meta.json").read_text(encoding="utf-8")))
    return out


def archive_client() -> Archive:
    """Archive over real HTTPS."""
    return Archive(HttpClient())
