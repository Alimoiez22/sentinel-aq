"""Data contract for OpenAQ daily files (docs/DATA_CONTRACT.md).

File level (decides whether a file can be read at all):
* it must be a readable gzip of UTF-8 CSV with a header line;
* its column SET must equal one KNOWN variant. Columns are then read by name, so a
  reordered file is accepted (F5 reorder), while an added, missing or renamed column
  makes the header unknown -> ContractError('unknown_header') -> escalate, never guessed.

Row level (handled by quality rules, M4): values and timestamps that do not parse are
kept as text and become NULL in the typed columns; the rules quarantine them.
"""

from __future__ import annotations

import csv
import gzip
import io
import math
import zlib
from dataclasses import dataclass

import pandera.polars as pa
import polars as pl

# Exact headers as observed (data/fixtures/HEADERS.md). Order is informative only.
KNOWN_HEADERS: dict[str, tuple[str, ...]] = {
    "A": ("location_id", "sensor_id", "location", "datetime", "lat", "lon", "parameter",
          "unit", "value"),
    "B": ("location_id", "sensors_id", "location", "datetime", "lat", "lon", "parameter",
          "units", "value"),
}  # fmt: skip
# Explicit mapping of each variant's names to canonical names.
TO_CANONICAL: dict[str, dict[str, str]] = {
    "A": {"location_id": "file_location_id", "sensor_id": "sensor_id", "location": "location_name",
          "datetime": "datetime_raw", "lat": "lat_raw", "lon": "lon_raw",
          "parameter": "parameter", "unit": "unit_raw", "value": "value_raw"},
    "B": {"location_id": "file_location_id", "sensors_id": "sensor_id", "location": "location_name",
          "datetime": "datetime_raw", "lat": "lat_raw", "lon": "lon_raw",
          "parameter": "parameter", "units": "unit_raw", "value": "value_raw"},
}  # fmt: skip
CONTRACT_VERSION = 1


class ContractError(Exception):
    """A file that cannot be read under the contract. `kind` drives classification."""

    def __init__(self, kind: str, detail: str) -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind, self.detail = kind, detail


@dataclass(frozen=True)
class ParsedFile:
    """A file read under the contract."""

    variant: str
    reordered: bool
    header: tuple[str, ...]
    frame: pl.DataFrame  # canonical columns, see CANONICAL_SCHEMA

    @property
    def row_count(self) -> int:
        return self.frame.height


CANONICAL_SCHEMA = pa.DataFrameSchema(
    {
        "row_no": pa.Column(pl.Int64, pa.Check.ge(1), unique=True, nullable=False),
        "file_location_id": pa.Column(pl.Int64, nullable=True),
        "sensor_id": pa.Column(pl.Int64, nullable=True),
        "location_name": pa.Column(pl.String, nullable=True),
        "datetime_raw": pa.Column(pl.String, nullable=True),
        "ts_utc": pa.Column(pl.Datetime("us", "UTC"), nullable=True),
        "utc_offset_s": pa.Column(pl.Int64, nullable=True),
        "lat": pa.Column(pl.Float64, nullable=True),
        "lon": pa.Column(pl.Float64, nullable=True),
        "parameter": pa.Column(pl.String, nullable=True),
        "unit_raw": pa.Column(pl.String, nullable=True),
        "value_raw": pa.Column(pl.String, nullable=True),
        "value": pa.Column(pl.Float64, nullable=True),
    },
    strict=True,
    ordered=True,
)


def identify_variant(header: tuple[str, ...]) -> tuple[str, bool]:
    """(variant, reordered) for a header; ContractError if the column set is unknown."""
    for name, cols in KNOWN_HEADERS.items():
        if header == cols:
            return name, False
        if len(header) == len(cols) and set(header) == set(cols):
            return name, True
    raise ContractError("unknown_header", ",".join(header))


def _decompress(raw_gz: bytes) -> str:
    try:
        return gzip.decompress(raw_gz).decode("utf-8")
    except (OSError, EOFError, zlib.error, UnicodeDecodeError) as exc:
        raise ContractError("unreadable", f"{type(exc).__name__}: {exc}") from exc


def _offset_seconds(col: pl.Expr) -> pl.Expr:
    """'+05:00' at the end of an ISO-8601 string -> 18000; NULL if absent."""
    m = col.str.extract_groups(r"([+-])(\d{2}):(\d{2})$")
    sign = pl.when(m.struct.field("1") == "-").then(-1).otherwise(1)
    return (
        sign * (m.struct.field("2").cast(pl.Int64) * 3600 + m.struct.field("3").cast(pl.Int64) * 60)
    ).cast(pl.Int64)


def parse_file(raw_gz: bytes) -> ParsedFile:
    """Read one gzipped day file into the canonical frame."""
    text = _decompress(raw_gz)
    first_line = text.split("\n", 1)[0]
    header = tuple(next(csv.reader([first_line]), []))
    if not header or header == ("",):
        raise ContractError("empty", "no header line")
    variant, reordered = identify_variant(header)
    df = pl.read_csv(io.StringIO(text), infer_schema=False).rename(TO_CANONICAL[variant])
    df = df.with_row_index("row_no", offset=1).with_columns(pl.col("row_no").cast(pl.Int64))
    value = pl.col("value_raw").str.strip_chars().cast(pl.Float64, strict=False)
    frame = df.select(
        "row_no",
        pl.col("file_location_id").cast(pl.Int64, strict=False),
        pl.col("sensor_id").cast(pl.Int64, strict=False),
        "location_name",
        "datetime_raw",
        pl.col("datetime_raw")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%:z", strict=False, time_unit="us")
        .dt.convert_time_zone("UTC")
        .alias("ts_utc"),
        _offset_seconds(pl.col("datetime_raw")).alias("utc_offset_s"),
        pl.col("lat_raw").cast(pl.Float64, strict=False).alias("lat"),
        pl.col("lon_raw").cast(pl.Float64, strict=False).alias("lon"),
        "parameter",
        "unit_raw",
        "value_raw",
        # NaN / inf are not measurements: treat them as unparseable (NULL).
        pl.when(value.is_finite()).then(value).otherwise(None).alias("value"),
    )
    return ParsedFile(variant, reordered, header, CANONICAL_SCHEMA.validate(frame))


def is_finite(x: float | None) -> bool:
    """True for a real number (used by tests)."""
    return x is not None and math.isfinite(x)
