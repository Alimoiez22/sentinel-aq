"""M2-M7 against PostgreSQL: register, idempotent loads, crashes, revisions, golden."""

from __future__ import annotations

import gzip
import io
import statistics
from datetime import UTC, date, datetime

import polars as pl
import psycopg
import pytest

from sentinel import db, faults_hooks
from sentinel.ops import UnsafeResetError, reset_database
from sentinel.pipeline import Pipeline
from sentinel.reconcile import compare_all, rebuild_golden
from tests.integration.conftest import SYNTH, connect_test, synthetic_files

pytestmark = pytest.mark.integration
Conn = psycopg.Connection[tuple[object, ...]]
T = datetime(2025, 11, 6, 7, 0, tzinfo=UTC)
LOC_A = 99000001


def one(conn: Conn, sql: str, *args: object) -> object:
    row = conn.execute(sql, args).fetchone()
    assert row is not None
    return row[0]


def load_all(conn: Conn, pipe: Pipeline) -> list[int]:
    ids = []
    for key, data in synthetic_files():
        reg = pipe.register(conn, key, data, T)
        assert pipe.process(conn, reg.batch_id)
        ids.append(reg.batch_id)
    return ids


def fingerprint(conn: Conn) -> tuple[object, ...]:
    """Order-independent summary of everything published."""
    return tuple(one(conn, q) for q in (
        "SELECT md5(string_agg(sensor_id || ts_utc::text || value || array_to_string(flags, ','),"
        " '|' ORDER BY sensor_id, ts_utc)) FROM wh.fact_measurement_hourly",
        "SELECT md5(string_agg(location_id || local_date::text || parameter"
        " || coalesce(daily_mean::text, '-') || publication_status, '|' ORDER BY 1))"
        " FROM wh.mart_daily_location",
        "SELECT count(*) FROM wh.fact_measurement_hourly",
    ))  # fmt: skip


def edit_values(data: bytes, factor: float, rows: slice) -> bytes:
    """A revised version of a file: multiply some values."""
    df = pl.read_csv(io.BytesIO(gzip.decompress(data)), infer_schema=False)
    vals = df["value"].to_list()
    for i in range(*rows.indices(len(vals))):
        vals[i] = f"{float(vals[i]) * factor:.1f}"
    out = io.StringIO()
    df.with_columns(pl.Series("value", vals)).write_csv(out, line_terminator="\n")
    return gzip.compress(out.getvalue().encode(), mtime=0)


# ------------------------------------------------------------------ M2 register
def test_migrations_are_recorded_and_guarded(conn: Conn, tmp_path: object) -> None:
    from pathlib import Path

    assert db.migrate(conn, Path("sql/ddl")) == []  # already applied: nothing to do
    d = Path(str(tmp_path)) / "ddl"
    d.mkdir()
    (d / "001_schemas.sql").write_text("SELECT 1;")  # same name, different content
    with pytest.raises(db.MigrationChangedError):
        db.migrate(conn, d)


def test_register_new_duplicate_and_rejected(conn: Conn, pipe: Pipeline) -> None:
    key, data = synthetic_files()[0]
    first = pipe.register(conn, key, data, T)
    again = pipe.register(conn, key, data, T)
    assert (first.outcome, again.outcome) == ("new", "duplicate")
    assert first.batch_id == again.batch_id
    bad = pipe.register(conn, key, data[: len(data) // 2], T)  # truncated gzip (F2)
    assert bad.outcome == "rejected" and bad.reason.startswith("unreadable")
    assert one(conn, "SELECT count(*) FROM ops.batch_manifest") == 2
    assert pipe.current_batch(conn, LOC_A, date(2025, 11, 1)) == first.batch_id


# --------------------------------------------- M3 idempotence, crashes, resume
def test_full_run_publishes_correct_means(conn: Conn, pipe: Pipeline) -> None:
    load_all(conn, pipe)
    assert one(conn, "SELECT count(*) FROM wh.fact_measurement_hourly") == 6 * 48
    # Independent check of one mart value straight from the fixture file.
    path = sorted(SYNTH.rglob("location-99000001-20251101.csv.gz"))[0]
    df = pl.read_csv(io.BytesIO(gzip.decompress(path.read_bytes())))
    expected = round(statistics.fmean(df.filter(pl.col("parameter") == "pm25")["value"]), 6)
    got = one(
        conn,
        "SELECT daily_mean FROM wh.mart_daily_location WHERE location_id = %s"
        " AND local_date = '2025-11-01' AND parameter = 'pm25'",
        LOC_A,
    )
    assert got == pytest.approx(expected, abs=1e-6)
    assert one(conn, "SELECT count(*) FROM ops.publication_status WHERE status = 'complete'") == 12
    assert one(conn, "SELECT count(*) FROM ops.batch_manifest WHERE status = 'published'") == 6


def test_replaying_every_stage_changes_nothing(conn: Conn, pipe: Pipeline) -> None:
    ids = load_all(conn, pipe)
    before = fingerprint(conn)
    for bid in ids:  # force every stage again, ignoring checkpoints
        pipe.stage(conn, bid)
        pipe.quality_and_fact(conn, bid)
        pipe.marts_and_gate(conn, bid)
        assert pipe.verify(conn, bid)
    assert fingerprint(conn) == before


def test_crash_after_staging_rolls_back_and_resumes(conn: Conn, pipe: Pipeline) -> None:
    key, data = synthetic_files()[0]
    bid = pipe.register(conn, key, data, T).batch_id
    faults_hooks.arm("after_staging_before_commit")
    with pytest.raises(faults_hooks.InjectedCrash):
        pipe.process(conn, bid)
    assert one(conn, "SELECT count(*) FROM staging.measurement") == 0  # nothing half-written
    assert pipe.done(conn, bid) == set()
    assert pipe.process(conn, bid)  # rerun completes


def test_crash_mid_marts_resumes_from_checkpoint(conn: Conn, pipe: Pipeline) -> None:
    key, data = synthetic_files()[0]
    bid = pipe.register(conn, key, data, T).batch_id
    faults_hooks.arm("mid_mart_refresh")
    with pytest.raises(faults_hooks.InjectedCrash):
        pipe.process(conn, bid)
    assert pipe.done(conn, bid) == {"staged", "quality", "fact"}
    assert one(conn, "SELECT count(*) FROM wh.mart_daily_location") == 0
    assert one(conn, "SELECT count(*) FROM ops.publication_status") == 0  # gate rolled back too
    assert pipe.process(conn, bid)
    assert pipe.done(conn, bid) == {"staged", "quality", "fact", "marts", "verified"}


def test_dropped_connection_then_retry(conn: Conn, pipe: Pipeline) -> None:
    key, data = synthetic_files()[0]
    bid = pipe.register(conn, key, data, T).batch_id
    faults_hooks.arm("after_staging_before_commit", how="drop_connection")
    with pytest.raises(psycopg.OperationalError):
        pipe.process(conn, bid)
    with connect_test() as fresh:  # the transient fault is gone on reconnect
        assert pipe.process(fresh, bid)
        assert one(fresh, "SELECT count(*) FROM wh.fact_measurement_hourly") == 48


# -------------------------------------------------- M4 quality, M6 revisions
def test_high_reject_rate_blocks_publication(conn: Conn, pipe: Pipeline) -> None:
    key, data = synthetic_files()[0]
    bad = edit_values(data, -1.0, slice(0, 20))  # 20 of 48 rows negative: 42% > 20%
    bid = pipe.register(conn, key, bad, T).batch_id
    assert pipe.process(conn, bid)
    assert (
        one(conn, "SELECT count(*) FROM quarantine.aq_rejects WHERE rule_id = 'Q03_negative'") == 20
    )
    assert one(conn, "SELECT status FROM ops.batch_manifest WHERE batch_id = %s", bid) == "blocked"
    rows = conn.execute(
        "SELECT parameter, daily_mean, publication_status FROM wh.mart_daily_location"
        " WHERE local_date = '2025-11-01' ORDER BY 1"
    ).fetchall()
    assert rows == [("pm10", None, "blocked"), ("pm25", None, "blocked")]


def test_revision_supersedes_diffs_and_marks_revised(conn: Conn, pipe: Pipeline) -> None:
    key, data = synthetic_files()[0]
    v1 = pipe.register(conn, key, data, T)
    assert pipe.process(conn, v1.batch_id)
    v2 = pipe.register(conn, key, edit_values(data, 2.0, slice(0, 3)), T)
    assert v2.outcome == "revision"
    assert pipe.process(conn, v2.batch_id)
    assert (
        one(conn, "SELECT status FROM ops.batch_manifest WHERE batch_id = %s", v1.batch_id)
        == "superseded"
    )
    assert one(conn, "SELECT count(*) FROM ops.revision_diff WHERE change = 'value_changed'") == 3
    assert (
        one(
            conn,
            "SELECT publication_status FROM wh.mart_daily_location WHERE parameter = 'pm25'"
            " AND local_date = '2025-11-01'",
        )
        == "revised"
    )


# ----------------------------------------------------- M7 golden == incremental
def test_incremental_equals_golden(conn: Conn, pipe: Pipeline) -> None:
    files = synthetic_files()
    key0, data0 = files[0]
    pipe.register(conn, key0, data0, T)  # v1 arrives first ...
    for key, data in reversed(files):  # ... others in reverse order
        pipe.register(conn, key, data, T)
    revised = edit_values(data0, 1.5, slice(5, 9))
    pipe.register(conn, key0, revised, T)  # then a revision of day 1
    for loc, d in {(99000001, date(2025, 11, x)) for x in (1, 2, 3)} | {
        (99000002, date(2025, 11, x)) for x in (1, 2, 3)
    }:
        bid = pipe.current_batch(conn, loc, d)
        assert bid is not None and pipe.process(conn, bid)
    inputs = {}
    for key, data in files:
        from sentinel.openaq.archive import parse_key

        inputs[parse_key(key)] = revised if key == key0 else data
    rebuild_golden(conn, pipe, inputs)
    diffs = compare_all(conn)
    assert all(d.equal and d.checksum_equal for d in diffs), [d for d in diffs if not d.equal]


def test_golden_detects_damage(conn: Conn, pipe: Pipeline) -> None:
    load_all(conn, pipe)
    from sentinel.openaq.archive import parse_key

    rebuild_golden(conn, pipe, {parse_key(k): d for k, d in synthetic_files()})
    # Delete by primary key: ctid is only unique within one partition.
    conn.execute(
        "DELETE FROM wh.fact_measurement_hourly WHERE (sensor_id, ts_utc) IN"
        " (SELECT sensor_id, ts_utc FROM wh.fact_measurement_hourly"
        " ORDER BY sensor_id, ts_utc LIMIT 2)"
    )
    conn.execute(
        "UPDATE wh.mart_daily_location SET daily_mean = daily_mean + 1"
        " WHERE daily_mean IS NOT NULL AND ctid IN (SELECT ctid FROM wh.mart_daily_location"
        " WHERE daily_mean IS NOT NULL LIMIT 1)"
    )
    conn.commit()
    fact, daily, _ = compare_all(conn)
    assert (fact.missing, fact.extra, fact.wrong, fact.checksum_equal) == (2, 0, 0, False)
    assert (daily.wrong, daily.checksum_equal) == (1, False)


def test_reset_refuses_live_warehouse() -> None:
    from sentinel.db import connect

    with connect() as live, pytest.raises(UnsafeResetError):
        reset_database(live)


def test_each_stage_is_committed_and_visible_to_other_sessions(conn: Conn, pipe: Pipeline) -> None:
    """Regression (found by the experiment runner): stages must COMMIT, not just create
    savepoints inside one long implicit transaction."""
    key, data = synthetic_files()[0]
    bid = pipe.register(conn, key, data, T).batch_id
    assert pipe.process(conn, bid)
    conn.rollback()  # discards nothing: every stage already committed
    with connect_test() as other:
        assert one(other, "SELECT count(*) FROM wh.fact_measurement_hourly") == 48
        assert (
            one(other, "SELECT status FROM ops.batch_manifest WHERE batch_id = %s", bid)
            == "published"
        )
