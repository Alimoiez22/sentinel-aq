"""Operational helpers: guideline and location seeding, landing runs, guarded reset."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from sentinel.openaq.archive import parse_key
from sentinel.pipeline import Conn, Pipeline, landing_files

RESETTABLE_DATABASES = frozenset({"warehouse_test", "warehouse_exp"})
RESET_TABLES = [
    "ops.action_log", "ops.incident", "ops.run_log", "ops.revision_diff",
    "ops.publication_status", "ops.quality_stats", "ops.checkpoint",
    "quarantine.aq_rejects", "staging.measurement", "wh.fact_measurement_hourly",
    "wh.mart_daily_location", "wh.mart_exceedance_summary", "wh.dim_sensor", "wh.dim_location",
    "golden.fact_measurement_hourly", "golden.mart_daily_location",
    "golden.mart_exceedance_summary", "golden.publication_status", "ops.batch_manifest",
]  # fmt: skip


class UnsafeResetError(RuntimeError):
    """Refused to reset a database that is not a test or experiment database."""


def load_guidelines(conn: Conn, path: Path = Path("config/guidelines.yaml")) -> int:
    """Upsert wh.guideline from the cited YAML file."""
    with path.open(encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    for g in doc["guidelines"]:
        conn.execute(
            "INSERT INTO wh.guideline (parameter, averaging, value_ugm3, source)"
            " VALUES (%s,%s,%s,%s)"
            " ON CONFLICT (parameter, averaging) DO UPDATE SET value_ugm3 = EXCLUDED.value_ugm3,"
            " source = EXCLUDED.source",
            (g["parameter"], g["averaging"], float(g["value_ugm3"]), doc["source"]),
        )
    conn.commit()
    return len(doc["guidelines"])


def seed_locations(conn: Conn, csv_path: Path, ids: Iterable[int] | None = None) -> int:
    """Upsert wh.dim_location from results/m1_locations.csv-shaped data."""
    df = pl.read_csv(csv_path, infer_schema_length=None)
    if ids is not None:
        df = df.filter(pl.col("location_id").is_in(list(ids)))
    n = 0
    for r in df.iter_rows(named=True):
        if not r.get("timezone"):
            raise ValueError(
                f"location {r['location_id']} has no timezone; cannot assign local days"
            )
        conn.execute(
            "INSERT INTO wh.dim_location"
            " (location_id, name, city, country, timezone, provider, geom)"
            " VALUES (%s,%s,%s,%s,%s,%s, CASE WHEN %s::float8 IS NULL THEN NULL ELSE"
            " ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326) END)"
            " ON CONFLICT (location_id) DO UPDATE SET name = EXCLUDED.name, city = EXCLUDED.city,"
            " country = EXCLUDED.country, timezone = EXCLUDED.timezone,"
            " provider = EXCLUDED.provider, geom = EXCLUDED.geom",
            (
                r["location_id"],
                r.get("name"),
                r.get("locality"),
                r.get("country_code"),
                r["timezone"],
                r.get("provider"),
                r.get("longitude"),
                r.get("longitude"),
                r.get("latitude"),
            ),
        )
        n += 1
    conn.commit()
    return n


def reset_database(conn: Conn) -> None:
    """Empty every pipeline table. Only allowed on warehouse_test / warehouse_exp (ADR-016)."""
    row = conn.execute("SELECT current_database()").fetchone()
    name = str(row[0]) if row else ""
    if name not in RESETTABLE_DATABASES:
        raise UnsafeResetError(f"refusing to reset database {name!r}")
    conn.execute("TRUNCATE " + ", ".join(RESET_TABLES) + " RESTART IDENTITY CASCADE")
    for (part,) in conn.execute(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'fact_measurement_hourly' "
        "AND c.relname <> 'fact_measurement_hourly_default'"
    ).fetchall():
        conn.execute(f'DROP TABLE wh."{part}"')  # monthly partitions are recreated on demand
    conn.commit()


@dataclass
class RunSummary:
    """What one `run` did."""

    registered: dict[str, int] = field(default_factory=dict)
    processed: list[tuple[int, date, int, bool]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)


def run_landing(
    conn: Conn,
    pipe: Pipeline,
    landing_root: Path,
    location_ids: list[int],
    start: date | None = None,
    end: date | None = None,
    raise_errors: bool = True,
) -> RunSummary:
    """Register every landing file in range, then process the current batch of each
    affected location-day. With raise_errors=False, failures are collected (M10 uses
    them as incident evidence) instead of stopping the run."""
    summary = RunSummary()
    touched: set[tuple[int, date]] = set()
    for key, path in landing_files(landing_root, location_ids):
        loc, d = parse_key(key)
        if (start and d < start) or (end and d > end):
            continue
        arrival = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        reg = pipe.register(conn, key, path.read_bytes(), arrival)
        summary.registered[reg.outcome] = summary.registered.get(reg.outcome, 0) + 1
        touched.add((loc, d))
    days = sorted(touched)
    for i, (loc, d) in enumerate(days):
        batch_id = pipe.current_batch(conn, loc, d)
        if batch_id is None:
            continue
        try:
            ok = pipe.process(conn, batch_id)
            summary.processed.append((loc, d, batch_id, ok))
        except Exception as exc:
            dead = conn.closed or conn.broken
            if not dead and conn.info.transaction_status != 0:
                conn.rollback()
            if raise_errors:
                raise
            summary.errors.append(_error(loc, d, batch_id, exc))
            if dead:
                # Every later location-day would fail the same way on this connection:
                # record them as skipped by the same (transient) cause, not as unknown.
                for loc2, d2 in days[i + 1 :]:
                    summary.errors.append(_error(loc2, d2, None, exc, skipped=True))
                break
    return summary


def reprocess(conn: Conn, pipe: Pipeline, location_ids: list[int]) -> dict[str, int]:
    """Replay quality, fact, marts and verification for the current batch of every
    location-day, ignoring checkpoints. Used after a quality-rule version change: the
    stages are idempotent (ADR-015), so replaying converges to what a fresh load gives."""
    rows = conn.execute(
        "SELECT DISTINCT ON (location_id, source_date) batch_id FROM ops.batch_manifest"
        " WHERE location_id = ANY(%s) AND status <> 'rejected'"
        " ORDER BY location_id, source_date, revision_no DESC",
        (location_ids,),
    ).fetchall()
    # Gate rows exist only for in-scope parameters; drop derived rows of any parameter
    # that left the scope, so the mart refresh does not resurrect them.
    removed = conn.execute(
        "DELETE FROM ops.publication_status WHERE location_id = ANY(%s)"
        " AND NOT (parameter = ANY(%s))",
        (location_ids, list(pipe.parameters)),
    ).rowcount
    conn.execute(
        "DELETE FROM wh.mart_daily_location WHERE location_id = ANY(%s)"
        " AND NOT (parameter = ANY(%s))",
        (location_ids, list(pipe.parameters)),
    )
    conn.execute(
        "DELETE FROM wh.mart_exceedance_summary WHERE location_id = ANY(%s)"
        " AND NOT (parameter = ANY(%s))",
        (location_ids, list(pipe.parameters)),
    )
    out = {"replayed": 0, "verified": 0, "blocked": 0, "out_of_scope_gate_rows_removed": removed}
    for (batch_id,) in rows:
        result = pipe.quality_and_fact(conn, batch_id)
        pipe.marts_and_gate(conn, batch_id)
        out["verified"] += int(pipe.verify(conn, batch_id))
        out["blocked"] += int(result.blocked)
        out["replayed"] += 1
    return out


def _error(
    loc: int, d: date, batch_id: int | None, exc: BaseException, skipped: bool = False
) -> dict[str, Any]:
    """Run-error evidence. `transient` uses the exception HIERARCHY: psycopg raises
    subclasses such as AdminShutdown (SQLSTATE 57P01) for a killed connection."""
    import psycopg

    return {
        "location_id": loc, "source_date": d.isoformat(), "batch_id": batch_id,
        "error": type(exc).__name__, "message": str(exc)[:500],
        "sqlstate": getattr(exc, "sqlstate", None),
        "transient": isinstance(exc, psycopg.OperationalError | psycopg.InterfaceError
                                | ConnectionError | TimeoutError),
        "skipped_after_connection_loss": skipped,
    }  # fmt: skip
