"""Quality rules (M4): quarantine, flag, block. Pure functions over the canonical frame.

Order of evaluation, and why:
1. Out-of-scope parameters are dropped first (Q16, counted) so they do not dilute the
   reject rate of the parameters we publish.
2. Row-level quarantine rules run in a fixed order; the FIRST failing rule is recorded
   (one quarantine row per file row), so per-rule counts add up to the reject total.
3. Duplicates (Q11) are resolved among surviving rows: the first row in file order is
   kept. Rule: deterministic and explainable; the quarantined copy keeps its payload.
4. Flags (Q12-Q15) never remove data; they are attached to fact rows.
5. Block (B01): reject rate over the threshold blocks publication of the location-day.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import polars as pl
import yaml
from pydantic import BaseModel, ConfigDict, Field

RuleAction = Literal["quarantine", "flag", "block", "info"]


class Rule(BaseModel):
    """One rule as listed in config/quality_rules.yaml."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    id: str
    version: int = Field(ge=1)
    action: RuleAction
    what: str


class Hampel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    half_window: int = Field(ge=1, le=12)
    n_sigmas: float = Field(gt=0)


class QualityConfig(BaseModel):
    """Validated config/quality_rules.yaml."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    version: int
    reject_rate_block_threshold: float = Field(gt=0, le=1)
    completeness_min_hours: int = Field(ge=1, le=24)
    ceiling_ugm3: dict[str, float]
    flag_band_ugm3: dict[str, float]
    unit_factors_to_ugm3: dict[str, float]
    stuck_min_hours: int = Field(ge=2)
    hampel: Hampel
    relocation_tolerance_deg: float = Field(gt=0)
    # Exact values that mean "no measurement" (v2, ADR-021). Such rows are quarantined
    # as Q00 but are NOT bad data: they lower completeness, not the reject rate.
    missing_value_codes: list[float] = Field(default_factory=list)
    rules: list[Rule]

    def rule(self, rule_id: str) -> Rule:
        for r in self.rules:
            if r.id == rule_id:
                return r
        raise KeyError(rule_id)


def load_quality_config(path: Path = Path("config/quality_rules.yaml")) -> QualityConfig:
    """Read and validate the rules file."""
    with path.open(encoding="utf-8") as f:
        return QualityConfig.model_validate(yaml.safe_load(f))


@dataclass(frozen=True)
class DayContext:
    """What the rules need to know about the file being checked."""

    location_id: int
    source_date: date
    timezone: str
    parameters: tuple[str, ...]
    now_utc: datetime
    # Registered coordinates of the location (wh.dim_location). Relocation is judged
    # against this fixed point, not against the last position seen, so the flag does not
    # depend on the order in which days are processed (needed for incremental == golden).
    site_position: tuple[float, float] | None = None


@dataclass
class QualityResult:
    """Accepted rows (with flags), quarantined rows, counts and the block decision."""

    accepted: pl.DataFrame
    rejects: pl.DataFrame
    counts: dict[str, int]
    in_scope_rows: int
    reject_rate: float
    blocked: bool
    unknown_units: list[str]
    relocations: dict[int, tuple[float, float]]


ACCEPTED_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "sensor_id": pl.Int64, "ts_utc": pl.Datetime("us", "UTC"), "parameter": pl.String,
    "value": pl.Float64, "value_orig": pl.Float64, "unit_orig": pl.String,
    "flags": pl.List(pl.String), "lat": pl.Float64, "lon": pl.Float64,
}  # fmt: skip
REJECT_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "row_no": pl.Int64, "rule_id": pl.String, "rule_version": pl.Int64,
    "reason": pl.String, "row_payload": pl.String,
}  # fmt: skip
PAYLOAD_COLS = ["sensor_id", "file_location_id", "datetime_raw", "parameter", "unit_raw",
                "value_raw", "lat", "lon", "location_name"]  # fmt: skip


def _row_checks(df: pl.DataFrame, cfg: QualityConfig, ctx: DayContext) -> list[tuple[str, pl.Expr]]:
    """(rule_id, failing-condition) in evaluation order."""
    factors = cfg.unit_factors_to_ugm3
    unit = pl.col("unit_raw").str.strip_chars()
    factor = unit.replace_strict(factors, default=None, return_dtype=pl.Float64)
    ugm3 = pl.col("value") * factor
    ceiling = pl.col("parameter").replace_strict(
        cfg.ceiling_ugm3, default=None, return_dtype=pl.Float64
    )
    local = pl.col("ts_utc").dt.convert_time_zone(ctx.timezone)
    expected_offset = (local.dt.base_utc_offset() + local.dt.dst_offset()).dt.total_seconds()
    hour_start_local_date = (local - pl.duration(hours=1)).dt.date()
    return [
        ("Q00_missing_code", pl.col("value").is_in(cfg.missing_value_codes)),
        ("Q10_bad_sensor", pl.col("sensor_id").is_null()),
        ("Q09_wrong_location", pl.col("file_location_id") != ctx.location_id),
        ("Q01_bad_timestamp", pl.col("ts_utc").is_null()),
        ("Q07_future", pl.col("ts_utc") > pl.lit(ctx.now_utc)),
        ("Q08_offset_mismatch", pl.col("utc_offset_s") != expected_offset),
        ("Q06_outside_local_day", hour_start_local_date != pl.lit(ctx.source_date)),
        ("Q02_bad_value", pl.col("value").is_null()),
        ("Q05_unknown_unit", factor.is_null()),
        ("Q03_negative", pl.col("value") < 0),
        ("Q04_above_ceiling", ugm3 > ceiling),
    ]


def _payload(df: pl.DataFrame) -> pl.Expr:
    return pl.struct([pl.col(c).cast(pl.String) for c in PAYLOAD_COLS]).struct.json_encode()


def evaluate(frame: pl.DataFrame, cfg: QualityConfig, ctx: DayContext) -> QualityResult:
    """Apply all rules to one location-day."""
    counts: dict[str, int] = {}
    in_scope = frame.filter(pl.col("parameter").is_in(list(ctx.parameters)))
    counts["Q16_out_of_scope"] = frame.height - in_scope.height

    df = in_scope.with_columns(pl.lit(None, dtype=pl.String).alias("_rule"))
    for rule_id, failing in _row_checks(df, cfg, ctx):
        df = df.with_columns(
            pl.when(pl.col("_rule").is_null() & failing.fill_null(False))
            .then(pl.lit(rule_id))
            .otherwise(pl.col("_rule"))
            .alias("_rule")
        )
    survivors = df.filter(pl.col("_rule").is_null()).sort("row_no")
    dup_mask = survivors.select(
        pl.struct("sensor_id", "ts_utc").is_duplicated()
        & ~pl.struct("sensor_id", "ts_utc").is_first_distinct()
    ).to_series()
    dups = survivors.filter(dup_mask).with_columns(pl.lit("Q11_duplicate").alias("_rule"))
    survivors = survivors.filter(~dup_mask)

    rejected = pl.concat([df.filter(pl.col("_rule").is_not_null()), dups])
    unknown_units = sorted(
        set(rejected.filter(pl.col("_rule") == "Q05_unknown_unit")["unit_raw"].drop_nulls())
    )
    for rid in rejected["_rule"].to_list():
        counts[rid] = counts.get(rid, 0) + 1
    rejects = (
        rejected.select(
            "row_no",
            pl.col("_rule").alias("rule_id"),
            pl.col("_rule")
            .map_elements(lambda r: cfg.rule(r).version, return_dtype=pl.Int64)
            .alias("rule_version"),
            pl.col("_rule")
            .map_elements(lambda r: cfg.rule(r).what, return_dtype=pl.String)
            .alias("reason"),
            _payload(rejected).alias("row_payload"),
        ).sort("row_no")
        if rejected.height
        else pl.DataFrame(schema=REJECT_SCHEMA)
    )

    accepted, relocations = _flag(survivors, cfg, ctx, counts)
    # Missing-value codes are absent hours, not bad rows: excluded from both sides.
    missing = counts.get("Q00_missing_code", 0)
    n = in_scope.height - missing
    rate = (rejected.height - missing) / n if n else 0.0
    blocked = n > 0 and rate > cfg.reject_rate_block_threshold
    if blocked:
        counts["B01_reject_rate"] = 1
    return QualityResult(accepted, rejects, counts, n, rate, blocked, unknown_units, relocations)


def _flag(
    df: pl.DataFrame, cfg: QualityConfig, ctx: DayContext, counts: dict[str, int]
) -> tuple[pl.DataFrame, dict[int, tuple[float, float]]]:
    """Convert units and attach flags to surviving rows."""
    factor = (
        pl.col("unit_raw")
        .str.strip_chars()
        .replace_strict(cfg.unit_factors_to_ugm3, default=None, return_dtype=pl.Float64)
    )
    base = df.with_columns((pl.col("value") * factor).alias("value_ugm3")).sort(
        ["sensor_id", "ts_utc"]
    )
    flags: dict[int, list[str]] = {int(r): [] for r in base["row_no"].to_list()}
    band = cfg.flag_band_ugm3
    for r in base.iter_rows(named=True):
        if r["value_ugm3"] > band.get(r["parameter"], float("inf")):
            flags[r["row_no"]].append("Q12_high_band")
    relocations: dict[int, tuple[float, float]] = {}
    for (sensor,), g in base.group_by(["sensor_id"], maintain_order=True):
        rows = g.sort("ts_utc").to_dicts()
        for i in _stuck(rows, cfg.stuck_min_hours):
            flags[rows[i]["row_no"]].append("Q13_stuck")
        for i in _hampel(rows, cfg.hampel.half_window, cfg.hampel.n_sigmas):
            flags[rows[i]["row_no"]].append("Q14_spike_hampel")
        moved = _relocated(rows, ctx.site_position, cfg.relocation_tolerance_deg)
        if moved is not None:
            relocations[int(sensor)] = moved
            for row in rows:
                flags[row["row_no"]].append("Q15_relocated")
    for fl in flags.values():
        for f in fl:
            counts[f] = counts.get(f, 0) + 1
    accepted = (
        base.select(
            "sensor_id",
            "ts_utc",
            "parameter",
            pl.col("value_ugm3").alias("value"),
            pl.col("value").alias("value_orig"),
            pl.col("unit_raw").alias("unit_orig"),
            pl.col("row_no")
            .replace_strict(
                {k: sorted(v) for k, v in flags.items()}, return_dtype=pl.List(pl.String)
            )
            .alias("flags"),
            "lat",
            "lon",
        )
        if base.height
        else pl.DataFrame(schema=ACCEPTED_SCHEMA)
    )
    return accepted.cast(ACCEPTED_SCHEMA), relocations  # type: ignore[arg-type]


def _stuck(rows: list[dict[str, object]], min_hours: int) -> list[int]:
    """Indexes of rows inside runs of >= min_hours identical values in consecutive hours."""
    out: list[int] = []
    start = 0
    for i in range(1, len(rows) + 1):
        same = (
            i < len(rows)
            and rows[i]["value_ugm3"] == rows[i - 1]["value_ugm3"]
            and rows[i]["ts_utc"] - rows[i - 1]["ts_utc"] == timedelta(hours=1)  # type: ignore[operator]
        )
        if not same:
            if i - start >= min_hours:
                out.extend(range(start, i))
            start = i
    return out


def _hampel(rows: list[dict[str, object]], k: int, n_sigmas: float) -> list[int]:
    """Classic Hampel identifier within one sensor-day (window clipped at day edges)."""
    xs = [float(r["value_ugm3"]) for r in rows]  # type: ignore[arg-type]
    out = []
    for i, x in enumerate(xs):
        window = xs[max(0, i - k) : i + k + 1]
        if len(window) < 3:
            continue
        med = statistics.median(window)
        mad = statistics.median(abs(w - med) for w in window)
        if mad > 0 and abs(x - med) > n_sigmas * 1.4826 * mad:
            out.append(i)
    return out


def _relocated(
    rows: list[dict[str, object]], known: tuple[float, float] | None, tol: float
) -> tuple[float, float] | None:
    """New (lat, lon) if the sensor moved beyond `tol` degrees from its known position."""
    if known is None or not rows:
        return None
    lat, lon = rows[0]["lat"], rows[0]["lon"]
    if not isinstance(lat, float) or not isinstance(lon, float):
        return None
    if abs(lat - known[0]) > tol or abs(lon - known[1]) > tol:
        return (lat, lon)
    return None


def rejects_to_rows(rejects: pl.DataFrame) -> list[tuple[object, ...]]:
    """Rows for INSERT into quarantine.aq_rejects (payload stays JSON text)."""
    return [
        (r["row_no"], r["rule_id"], r["rule_version"], r["reason"], json.loads(r["row_payload"]))
        for r in rejects.iter_rows(named=True)
    ]
