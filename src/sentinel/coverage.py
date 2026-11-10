"""Coverage measurements for choosing study locations (M1).

Two levels, so the expensive one is only paid for shortlisted locations:

1. listing coverage: from S3 listings only: which days have a file, its size, and the
   arrival lag = LastModified - end of the local day. Caveat: LastModified is the time
   of the LATEST write, so for a patched file the lag shows the patch, not first arrival.
2. hours coverage: from downloaded files: distinct hourly timestamps per parameter per
   day. A daily mean needs >= 18 of 24 hours (EU 2008/50/EC, RESEARCH §2.9).

Header variants: only the two variants recorded in data/fixtures/HEADERS.md are read.
Anything else is reported as `unknown` with its header text, never guessed (M2 builds
the formal contract from what is found here).
"""

from __future__ import annotations

import csv
import gzip
import io
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import polars as pl

from sentinel.openaq.archive import ObjectInfo, parse_key

KNOWN_HEADERS: dict[str, tuple[str, ...]] = {
    "A": ("location_id", "sensor_id", "location", "datetime", "lat", "lon", "parameter",
          "unit", "value"),
    "B": ("location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter",
          "units", "value"),
}  # fmt: skip
MIN_HOURS_FOR_DAILY_MEAN = 18
WINTER_MONTHS = frozenset({11, 12, 1, 2})  # smog season used in the report (M1 choice)


def listing_rows(
    location_id: int, objects: list[ObjectInfo], tz_name: str
) -> list[dict[str, object]]:
    """One row per day file of a location, with arrival lag in hours."""
    tz = ZoneInfo(tz_name) if tz_name else None
    rows = []
    for obj in objects:
        loc, day = parse_key(obj.key)
        if loc != location_id:
            raise ValueError(f"{obj.key} does not belong to location {location_id}")
        lag = None
        if tz is not None:
            day_end = datetime.combine(day + timedelta(days=1), time(0), tzinfo=tz)
            lag = round((obj.last_modified - day_end).total_seconds() / 3600, 2)
        rows.append({
            "location_id": location_id, "source_date": day.isoformat(), "key": obj.key,
            "size_bytes": obj.size, "etag": obj.etag,
            "last_modified_utc": obj.last_modified.isoformat(), "arrival_lag_h": lag,
        })  # fmt: skip
    return rows


@dataclass(frozen=True)
class HeaderInfo:
    """Which known variant a header is, or 'unknown'."""

    variant: str
    header: tuple[str, ...]


def detect_header(raw_gz: bytes) -> HeaderInfo:
    """Read only the first CSV line of a gzipped file."""
    with gzip.open(io.BytesIO(raw_gz), "rt", encoding="utf-8", newline="") as f:
        header = tuple(next(csv.reader(f), []))
    for name, cols in KNOWN_HEADERS.items():
        if header == cols:
            return HeaderInfo(name, header)
    return HeaderInfo("unknown", header)


def file_hours(raw_gz: bytes, key: str) -> list[dict[str, object]]:
    """Distinct hourly timestamps per parameter in one day file."""
    location_id, day = parse_key(key)
    info = detect_header(raw_gz)
    base = {
        "location_id": location_id,
        "source_date": day.isoformat(),
        "key": key,
        "header_variant": info.variant,
    }
    if info.variant == "unknown":
        return [
            {
                **base,
                "parameter": "",
                "n_sensors": 0,
                "n_hours": 0,
                "n_rows": 0,
                "n_bad_datetime": 0,
                "header": ",".join(info.header),
            }
        ]
    df = pl.read_csv(io.BytesIO(gzip.decompress(raw_gz)), infer_schema=False)
    df = df.rename({"sensor_id": "sensor", "sensors_id": "sensor"}, strict=False)
    df = df.with_columns(
        pl.col("datetime").str.to_datetime("%Y-%m-%dT%H:%M:%S%:z", strict=False).alias("ts")
    )
    agg = (
        df.group_by("parameter")
        .agg(
            pl.col("sensor").n_unique().alias("n_sensors"),
            pl.col("ts").drop_nulls().dt.truncate("1h").n_unique().alias("n_hours"),
            pl.len().alias("n_rows"),
            pl.col("ts").is_null().sum().alias("n_bad_datetime"),
        )
        .sort("parameter")
    )
    return [{**base, **r, "header": ""} for r in agg.to_dicts()]


def summarise(
    locations: pl.DataFrame,
    listing: pl.DataFrame,
    hours: pl.DataFrame | None,
    start: date,
    end: date,
    parameters: list[str],
) -> pl.DataFrame:
    """Per-location summary used by the M1 report. Denominators are explicit columns."""
    days_in_range = (end - start).days + 1
    winter_days = sum(
        1 for i in range(days_in_range) if (start + timedelta(days=i)).month in WINTER_MONTHS
    )
    lst = listing.with_columns(pl.col("source_date").str.to_date())
    per_loc = lst.group_by("location_id").agg(
        pl.len().alias("days_with_file"),
        pl.col("source_date")
        .filter(pl.col("source_date").dt.month().is_in(list(WINTER_MONTHS)))
        .len()
        .alias("winter_days_with_file"),
        pl.col("arrival_lag_h").median().alias("median_arrival_lag_h"),
        pl.col("arrival_lag_h").max().alias("max_arrival_lag_h"),
        pl.col("size_bytes").sum().alias("total_bytes"),
    )
    out = locations.join(per_loc, on="location_id", how="left").with_columns(
        pl.lit(days_in_range).alias("days_in_range"),
        pl.lit(winter_days).alias("winter_days_in_range"),
        pl.col("days_with_file").fill_null(0),
        pl.col("winter_days_with_file").fill_null(0),
    )
    if hours is not None and hours.height > 0:
        h = hours.filter(pl.col("parameter").is_in(parameters)).with_columns(
            pl.col("source_date").str.to_date()
        )
        for p in parameters:
            hp = h.filter(pl.col("parameter") == p)
            agg = hp.group_by("location_id").agg(
                pl.len().alias(f"{p}_days_with_hours"),
                (pl.col("n_hours") >= MIN_HOURS_FOR_DAILY_MEAN).sum().alias(f"{p}_days_ge18h"),
                ((pl.col("n_hours") >= MIN_HOURS_FOR_DAILY_MEAN)
                 & pl.col("source_date").dt.month().is_in(list(WINTER_MONTHS)))
                .sum().alias(f"{p}_winter_days_ge18h"),
            )  # fmt: skip
            out = out.join(agg, on="location_id", how="left")
    return out.sort(["distance_lahore_km", "location_id"], nulls_last=True)
