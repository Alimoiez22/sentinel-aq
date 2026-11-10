"""The incremental pipeline (M2-M6): register -> stage -> quality+fact -> marts+gate -> verify.

Correctness mechanisms (PROJECT_BRIEF §10), each explained where it is used:
* register: content-addressed. The same bytes for a location-day are one batch, so a
  duplicate file is a no-op (F1). A new checksum for a known location-day is a revision.
* every stage runs in ONE transaction under a transaction-level advisory lock on the
  location-day and writes its checkpoint inside that transaction. A crash before commit
  (F10, F11) leaves no partial state and no checkpoint, so rerunning repeats the stage.
* every write REPLACES the location-day (delete + insert), so running a stage twice gives
  the same rows as running it once (idempotence; property-tested in M3).
* only the latest non-rejected revision of a location-day is processed; older ones are
  marked superseded when the newer one reaches the fact table.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
from psycopg.types.json import Jsonb

from sentinel import faults_hooks
from sentinel.contract import ContractError, parse_file
from sentinel.db import lock_location_day, sql_file
from sentinel.log import get_logger
from sentinel.openaq.archive import parse_key
from sentinel.quality import DayContext, QualityConfig, QualityResult, evaluate, rejects_to_rows
from sentinel.rawstore import RawStore

Conn = psycopg.Connection[tuple[Any, ...]]
STAGES = ("staged", "quality", "fact", "marts", "verified")


@dataclass(frozen=True)
class Registered:
    """Outcome of registering one file."""

    batch_id: int
    location_id: int
    source_date: date
    outcome: str  # new | duplicate | revision | rejected
    reason: str = ""


@dataclass
class Pipeline:
    """One configured pipeline. `schema` is 'wh' (live) or 'golden' for marts/fact."""

    store: RawStore
    cfg: QualityConfig
    parameters: tuple[str, ...]
    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    log: Any = field(default_factory=lambda: get_logger(component="pipeline"))

    # ---------------------------------------------------------------- register (M2)
    def register(self, conn: Conn, key: str, data: bytes, arrival_ts: datetime) -> Registered:
        """Record one file version in ops.batch_manifest."""
        location_id, source_date = parse_key(key)
        sha, _ = self.store.put(data)
        with conn.transaction():
            lock_location_day(conn, location_id, source_date.toordinal())
            row = conn.execute(
                "SELECT batch_id, status FROM ops.batch_manifest "
                "WHERE location_id = %s AND source_date = %s AND sha256 = %s",
                (location_id, source_date, sha),
            ).fetchone()
            if row is not None:
                return Registered(row[0], location_id, source_date, "duplicate")
            variant: str | None = None
            n_rows: int | None = None
            status, reason = "registered", ""
            try:
                parsed = parse_file(data)
                variant, n_rows = parsed.variant, parsed.row_count
            except ContractError as exc:
                status, reason = "rejected", f"{exc.kind}: {exc.detail}"[:500]
            prev = conn.execute(
                "SELECT coalesce(max(revision_no), -1) FROM ops.batch_manifest "
                "WHERE location_id = %s AND source_date = %s",
                (location_id, source_date),
            ).fetchone()
            revision_no = int(prev[0]) + 1 if prev else 0
            (batch_id,) = conn.execute(
                "INSERT INTO ops.batch_manifest (location_id, source_date, file_key, sha256,"
                " size_bytes, raw_row_count, header_variant, arrival_ts, status, status_reason,"
                " revision_no) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING batch_id",
                (
                    location_id,
                    source_date,
                    key,
                    sha,
                    len(data),
                    n_rows,
                    variant,
                    arrival_ts,
                    status,
                    reason or None,
                    revision_no,
                ),
            ).fetchone() or (None,)
        outcome = "rejected" if status == "rejected" else ("revision" if revision_no else "new")
        self.log.info("registered", batch_id=batch_id, key=key, outcome=outcome, reason=reason)
        return Registered(int(batch_id), location_id, source_date, outcome, reason)

    # ------------------------------------------------------------------ helpers
    def batch(self, conn: Conn, batch_id: int) -> dict[str, Any]:
        cur = conn.execute(
            "SELECT batch_id, location_id, source_date, sha256, status, revision_no "
            "FROM ops.batch_manifest WHERE batch_id = %s",
            (batch_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"batch {batch_id} not found")
        return dict(zip([d.name for d in cur.description or []], row, strict=True))

    def current_batch(self, conn: Conn, location_id: int, source_date: date) -> int | None:
        """Latest non-rejected revision of a location-day."""
        row = conn.execute(
            "SELECT batch_id FROM ops.batch_manifest WHERE location_id = %s AND source_date = %s"
            " AND status <> 'rejected' ORDER BY revision_no DESC LIMIT 1",
            (location_id, source_date),
        ).fetchone()
        return int(row[0]) if row else None

    def done(self, conn: Conn, batch_id: int) -> set[str]:
        return {
            r[0]
            for r in conn.execute(
                "SELECT stage FROM ops.checkpoint WHERE batch_id = %s", (batch_id,)
            ).fetchall()
        }

    def _checkpoint(self, conn: Conn, batch_id: int, stage: str, detail: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO ops.checkpoint (batch_id, stage, detail) VALUES (%s, %s, %s) "
            "ON CONFLICT (batch_id, stage) DO UPDATE SET done_at = now(), detail = EXCLUDED.detail",
            (batch_id, stage, Jsonb(detail)),
        )

    def context(self, conn: Conn, location_id: int, source_date: date) -> DayContext:
        """Everything the quality rules need about a location-day."""
        return day_context(conn, location_id, source_date, self.parameters, self.now())

    # ------------------------------------------------------------- stage (M3)
    def stage(self, conn: Conn, batch_id: int) -> int:
        """Replace staging rows of the batch's location-day with this batch's rows."""
        b = self.batch(conn, batch_id)
        parsed = parse_file(self.store.get(b["sha256"]))
        with conn.transaction():
            lock_location_day(conn, b["location_id"], b["source_date"].toordinal())
            conn.execute(
                "DELETE FROM staging.measurement WHERE location_id = %s AND source_date = %s",
                (b["location_id"], b["source_date"]),
            )
            cols = ["row_no", "sensor_id", "location_name", "datetime_raw", "ts_utc",
                    "utc_offset_s",
                    "lat", "lon", "parameter", "unit_raw", "value_raw", "value"]  # fmt: skip
            with conn.cursor().copy(
                "COPY staging.measurement (batch_id, location_id, source_date, "
                + ", ".join(cols)
                + ") FROM STDIN"
            ) as cp:
                for r in parsed.frame.select(cols).iter_rows():
                    cp.write_row((batch_id, b["location_id"], b["source_date"], *r))
            faults_hooks.crashpoint("after_staging_before_commit", conn)
            conn.execute(
                "UPDATE ops.batch_manifest SET attempt_count = attempt_count + 1 "
                "WHERE batch_id = %s",
                (batch_id,),
            )
            self._checkpoint(
                conn, batch_id, "staged", {"rows": parsed.row_count, "variant": parsed.variant}
            )
        return parsed.row_count

    def _staged_frame(self, conn: Conn, b: dict[str, Any]) -> pl.DataFrame:
        cur = conn.execute(
            "SELECT row_no::bigint, location_id::bigint AS file_location_id, sensor_id::bigint,"
            " location_name, datetime_raw, ts_utc, utc_offset_s::bigint, lat, lon, parameter,"
            " unit_raw, value_raw, value FROM staging.measurement"
            " WHERE batch_id = %s ORDER BY row_no",
            (b["batch_id"],),
        )
        names = [d.name for d in cur.description or []]
        return pl.DataFrame(cur.fetchall(), schema=names, orient="row", infer_schema_length=None)

    # --------------------------------------------- quality + fact (M4, M5, M6)
    def quality_and_fact(self, conn: Conn, batch_id: int) -> QualityResult:
        """Apply rules to staged rows; replace quarantine, stats, fact and sensors at once."""
        b = self.batch(conn, batch_id)
        # Re-parse from the immutable raw file: staging holds the same rows, but reading
        # the bytes again keeps the file-level `file_location_id` (staging stores the key's).
        frame = parse_file(self.store.get(b["sha256"])).frame
        result = evaluate(frame, self.cfg, self.context(conn, b["location_id"], b["source_date"]))
        with conn.transaction():
            lock_location_day(conn, b["location_id"], b["source_date"].toordinal())
            self._write_quality(conn, b, result)
            old = self._fact_rows(conn, "wh", b["location_id"], b["source_date"])
            write_fact(conn, "wh", b, result)
            self._write_revision_diff(conn, b, old)
            self._update_sensors(conn, b, result)
            conn.execute(
                "UPDATE ops.batch_manifest SET status = 'superseded', superseded_by = %s "
                "WHERE location_id = %s AND source_date = %s AND batch_id <> %s "
                "AND status <> 'rejected' AND revision_no < %s",
                (batch_id, b["location_id"], b["source_date"], batch_id, b["revision_no"]),
            )
            conn.execute(
                "UPDATE ops.batch_manifest SET status = %s WHERE batch_id = %s",
                ("blocked" if result.blocked else "loaded", batch_id),
            )
            detail = {
                "counts": result.counts,
                "reject_rate": round(result.reject_rate, 6),
                "blocked": result.blocked,
                "unknown_units": result.unknown_units,
            }
            self._checkpoint(conn, batch_id, "quality", detail)
            self._checkpoint(conn, batch_id, "fact", {"rows": result.accepted.height})
        return result

    def _write_quality(self, conn: Conn, b: dict[str, Any], result: QualityResult) -> None:
        conn.execute("DELETE FROM quarantine.aq_rejects WHERE batch_id = %s", (b["batch_id"],))
        conn.execute("DELETE FROM ops.quality_stats WHERE batch_id = %s", (b["batch_id"],))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO quarantine.aq_rejects (batch_id, location_id, source_date, row_no,"
                " rule_id, rule_version, reason, row_payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                [(b["batch_id"], b["location_id"], b["source_date"], rn, rid, ver, why, Jsonb(p))
                 for rn, rid, ver, why, p in rejects_to_rows(result.rejects)])  # fmt: skip
            actions = {r.id: r.action for r in self.cfg.rules}
            cur.executemany(
                "INSERT INTO ops.quality_stats (batch_id, rule_id, action, n_rows)"
                " VALUES (%s,%s,%s,%s)",
                [(b["batch_id"], rid, actions.get(rid, "info"), n)
                 for rid, n in sorted(result.counts.items())])  # fmt: skip

    def _fact_rows(
        self, conn: Conn, schema: str, location_id: int, source_date: date
    ) -> dict[tuple[int, datetime], tuple[float, int]]:
        rows = conn.execute(
            f"SELECT sensor_id, ts_utc, value, batch_id FROM {schema}.fact_measurement_hourly "  # noqa: S608 - schema is 'wh' or 'golden'
            "WHERE location_id = %s AND source_date = %s",
            (location_id, source_date),
        ).fetchall()
        return {(int(s), ts): (float(v), int(bid)) for s, ts, v, bid in rows}

    def _write_revision_diff(
        self, conn: Conn, b: dict[str, Any], old: dict[tuple[int, datetime], tuple[float, int]]
    ) -> None:
        old_batches = {bid for _, bid in old.values()} - {b["batch_id"]}
        if not old_batches:
            return  # first load, or a re-run of the same batch: nothing to compare
        new = self._fact_rows(conn, "wh", b["location_id"], b["source_date"])
        old_batch = max(old_batches)
        conn.execute("DELETE FROM ops.revision_diff WHERE new_batch_id = %s", (b["batch_id"],))
        rows: list[tuple[tuple[int, datetime], str, float | None, float | None]] = []
        for k in sorted(old.keys() | new.keys()):
            o, n = old.get(k), new.get(k)
            if o is None and n is not None:
                rows.append((k, "added", None, n[0]))
            elif n is None and o is not None:
                rows.append((k, "removed", o[0], None))
            elif o is not None and n is not None and o[0] != n[0]:
                rows.append((k, "value_changed", o[0], n[0]))
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO ops.revision_diff (location_id, source_date, old_batch_id,"
                " new_batch_id, sensor_id, ts_utc, change, old_value, new_value)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(b["location_id"], b["source_date"], old_batch, b["batch_id"], k[0], k[1],
                  ch, ov, nv)
                 for k, ch, ov, nv in rows])  # fmt: skip

    def _update_sensors(self, conn: Conn, b: dict[str, Any], result: QualityResult) -> None:
        """Upsert sensors; a relocation closes the current position version, opens a new one."""
        acc = result.accepted
        if acc.height == 0:
            return
        per_sensor = acc.group_by("sensor_id", "parameter").agg(
            pl.col("ts_utc").min().alias("first"),
            pl.col("ts_utc").max().alias("last"),
            pl.col("lat").first(),
            pl.col("lon").first(),
        )
        for r in per_sensor.iter_rows(named=True):
            sid = int(r["sensor_id"])
            if sid in result.relocations:
                conn.execute(
                    "UPDATE wh.dim_sensor SET valid_to = %s WHERE sensor_id = %s "
                    "AND valid_to IS NULL",
                    (r["first"], sid),
                )
            conn.execute(
                "INSERT INTO wh.dim_sensor (sensor_id, valid_from, location_id, parameter, geom,"
                " first_seen, last_seen) SELECT %(s)s, %(f)s, %(loc)s, %(p)s,"
                " ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326), %(f)s, %(l)s"
                " WHERE NOT EXISTS (SELECT 1 FROM wh.dim_sensor WHERE sensor_id = %(s)s"
                " AND valid_to IS NULL)",
                {
                    "s": sid,
                    "f": r["first"],
                    "l": r["last"],
                    "loc": b["location_id"],
                    "p": r["parameter"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                },
            )
            conn.execute(
                "UPDATE wh.dim_sensor SET first_seen = least(first_seen, %(f)s),"
                " last_seen = greatest(last_seen, %(l)s)"
                " WHERE sensor_id = %(s)s AND valid_to IS NULL",
                {"s": sid, "f": r["first"], "l": r["last"]},
            )

    # -------------------------------------------------- marts + gate (M5, M8)
    def marts_and_gate(self, conn: Conn, batch_id: int) -> dict[str, str]:
        """Decide publication status per parameter, then refresh marts for affected days."""
        b = self.batch(conn, batch_id)
        with conn.transaction():
            lock_location_day(conn, b["location_id"], b["source_date"].toordinal())
            statuses = gate(conn, "wh", b, self.parameters, self.cfg.completeness_min_hours)
            faults_hooks.crashpoint("mid_mart_refresh", conn)
            refresh_marts(
                conn, "wh", b["location_id"], b["source_date"], self.cfg.completeness_min_hours
            )
            self._checkpoint(conn, batch_id, "marts", statuses)
        return statuses

    # ---------------------------------------------------------------- verify
    def verify(self, conn: Conn, batch_id: int) -> bool:
        """Recompute the location-day from the raw file and compare with what was published."""
        b = self.batch(conn, batch_id)
        expected = evaluate(
            parse_file(self.store.get(b["sha256"])).frame,
            self.cfg,
            self.context(conn, b["location_id"], b["source_date"]),
        ).accepted
        actual = self._fact_rows(conn, "wh", b["location_id"], b["source_date"])
        exp = {(int(r["sensor_id"]), r["ts_utc"]): float(r["value"])
               for r in expected.iter_rows(named=True)}  # fmt: skip
        ok = exp == {k: v[0] for k, v in actual.items()} and all(
            bid == batch_id for _, bid in actual.values()
        )
        marts_ok = self.done(conn, batch_id) >= {"marts"}
        with conn.transaction():
            if ok and marts_ok:
                final = (
                    "blocked" if self.batch(conn, batch_id)["status"] == "blocked" else "published"
                )
                conn.execute(
                    "UPDATE ops.batch_manifest SET status = %s WHERE batch_id = %s",
                    (final, batch_id),
                )
                self._checkpoint(conn, batch_id, "verified", {"rows": len(exp)})
        return ok and marts_ok

    # ------------------------------------------------------------- run all stages
    def process(self, conn: Conn, batch_id: int) -> bool:
        """Run the stages not yet checkpointed (resume), in order. Returns verified."""
        done = self.done(conn, batch_id)
        if "staged" not in done:
            self.stage(conn, batch_id)
        if "fact" not in done:
            self.quality_and_fact(conn, batch_id)
        if "marts" not in done:
            self.marts_and_gate(conn, batch_id)
        return self.verify(conn, batch_id)


def day_context(
    conn: Conn, location_id: int, source_date: date, parameters: tuple[str, ...], now: datetime
) -> DayContext:
    """DayContext from wh.dim_location (time zone and registered coordinates)."""
    row = conn.execute(
        "SELECT timezone, ST_Y(geom), ST_X(geom) FROM wh.dim_location WHERE location_id = %s",
        (location_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"location {location_id} is not in wh.dim_location (seed it first)")
    site = (float(row[1]), float(row[2])) if row[1] is not None else None
    return DayContext(location_id, source_date, str(row[0]), parameters, now, site)


def write_fact(conn: Conn, schema: str, b: dict[str, Any], result: QualityResult) -> None:
    """Replace the fact rows of one location-day in `schema` (wh or golden)."""
    acc = result.accepted
    conn.execute(
        f"DELETE FROM {schema}.fact_measurement_hourly "  # noqa: S608 - fixed schema names
        "WHERE location_id = %s AND source_date = %s",
        (b["location_id"], b["source_date"]),
    )
    if acc.height == 0:
        return
    if schema == "wh":
        for m in sorted({ts.date().replace(day=1) for ts in acc["ts_utc"].to_list()}):
            conn.execute("SELECT wh.ensure_month_partition(%s)", (m,))
    with conn.cursor().copy(
        f"COPY {schema}.fact_measurement_hourly (sensor_id, ts_utc, location_id, source_date,"
        " parameter, value, value_orig, unit_orig, flags, batch_id, revision_no) FROM STDIN"
    ) as cp:
        for r in acc.iter_rows(named=True):
            cp.write_row(
                (
                    r["sensor_id"],
                    r["ts_utc"],
                    b["location_id"],
                    b["source_date"],
                    r["parameter"],
                    r["value"],
                    r["value_orig"],
                    r["unit_orig"],
                    r["flags"],
                    b["batch_id"],
                    b["revision_no"],
                )
            )


def gate(
    conn: Conn, schema: str, b: dict[str, Any], parameters: tuple[str, ...], min_hours: int
) -> dict[str, str]:
    """Publication gate for the batch's location-day (local_date = source_date)."""
    table = "ops.publication_status" if schema == "wh" else "golden.publication_status"
    blocked = conn.execute(
        "SELECT status FROM ops.batch_manifest WHERE batch_id = %s", (b["batch_id"],)
    ).fetchone()
    is_blocked = schema == "wh" and blocked is not None and blocked[0] == "blocked"
    if schema == "golden":
        is_blocked = bool(b.get("blocked"))
    hours: dict[str, int] = dict(
        conn.execute(
            f"SELECT parameter, count(DISTINCT ts_utc) FROM {schema}.fact_measurement_hourly "  # noqa: S608
            "WHERE location_id = %s AND source_date = %s GROUP BY parameter",
            (b["location_id"], b["source_date"]),
        ).fetchall()
    )
    was_published = (
        schema == "wh"
        and conn.execute(
            "SELECT 1 FROM ops.batch_manifest WHERE location_id = %s AND source_date = %s "
            "AND batch_id <> %s AND superseded_by IS NOT NULL AND status = 'superseded' "
            "AND EXISTS (SELECT 1 FROM ops.checkpoint c WHERE c.batch_id = batch_manifest.batch_id "
            "AND c.stage = 'verified')",
            (b["location_id"], b["source_date"], b["batch_id"]),
        ).fetchone()
        is not None
    )
    out: dict[str, str] = {}
    for p in parameters:
        h = int(hours.get(p, 0))
        if is_blocked:
            status, reason = "blocked", "reject rate above threshold (B01)"
        elif h < min_hours:
            status, reason = "partial", f"{h} of 24 hours < {min_hours}"
        elif was_published:
            status, reason = "revised", "replaced an earlier published revision"
        else:
            status, reason = "complete", f"{h} of 24 hours"
        conn.execute(
            f"INSERT INTO {table} (location_id, local_date, parameter, status, reason, batch_id,"  # noqa: S608
            " revision_no, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s, now()) "
            "ON CONFLICT (location_id, local_date, parameter)"
            " DO UPDATE SET status = EXCLUDED.status,"
            " reason = EXCLUDED.reason, batch_id = EXCLUDED.batch_id,"
            " revision_no = EXCLUDED.revision_no, updated_at = now()",
            (
                b["location_id"],
                b["source_date"],
                p,
                status,
                reason,
                b["batch_id"] if schema == "wh" else None,
                b["revision_no"],
            ),
        )
        out[p] = status
    return out


def refresh_marts(
    conn: Conn, schema: str, location_id: int, source_date: date, min_hours: int
) -> None:
    """Run sql/marts/refresh_daily.sql for the day and its neighbours (local-date spill)."""
    dates = [source_date - timedelta(days=1), source_date, source_date + timedelta(days=1)]
    with psycopg.ClientCursor(conn) as cur:  # client-side binding: the file has several statements
        cur.execute(f"SET LOCAL search_path TO {schema}, public")
        cur.execute(
            sql_file("marts/refresh_daily.sql"),  # trusted repository file
            {"location_id": location_id, "dates": dates, "min_hours": min_hours},
        )
        cur.execute("SET LOCAL search_path TO DEFAULT")


def landing_files(landing_root: Path, location_ids: list[int]) -> list[tuple[str, Path]]:
    """(archive key, path) of every day file in the landing mirror for the locations."""
    out = []
    for loc in sorted(set(location_ids)):
        base = landing_root / "records" / "csv.gz" / f"locationid={loc}"
        for p in sorted(base.rglob("*.csv.gz")):
            out.append((p.relative_to(landing_root).as_posix(), p))
    return out
