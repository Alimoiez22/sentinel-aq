"""Track A experiment runner (M11): faults x modes x seeds -> results.csv + metadata.json.

Metric definitions (docs/EXPERIMENTS.md):
* outputs_correct    - every published table equals golden built from the trial's truth
* recovered_verified - outputs_correct AND nothing was escalated (fixed without a human)
* escalated          - at least one incident escalated to a human
* handled_as_expected - outputs_correct AND escalated == the fault's expected escalation
  (faults.EXPECTED). A pipeline that silently blocks bad data without telling anyone
  scores False here, even though its tables are correct
* damage             - missing / extra / wrong rows in fact and marts vs golden
* inappropriate_actions - automatic actions (not escalations) taken on a fault whose
  correct handling (faults.EXPECTED) is not automatic, or after misclassification; for
  retries_only: reruns of a fault that is not transient or a crash
* time_to_verified_recovery_s - guarded: max over recovered incidents of detection ->
  verification; retries_only: first run error -> first clean rerun. Blank when not
  recovered; 0 when nothing needed recovery
* wall_s / overhead_s - trial wall time, and its excess over the mean fault-free (F0)
  trial of the same mode
"""

from __future__ import annotations

import json
import platform
import shutil
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from importlib.metadata import version
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
import yaml

from sentinel import db, faults_hooks
from sentinel.faults import N_RUNS, Trial, build_trial
from sentinel.ops import load_guidelines, reset_database, seed_locations
from sentinel.pipeline import Conn, Pipeline
from sentinel.provenance import git_commit
from sentinel.quality import load_quality_config
from sentinel.rawstore import RawStore, sha256_hex, write_atomic
from sentinel.reconcile import compare_all, damage, rebuild_golden
from sentinel.recovery import (
    DictSource,
    Engine,
    load_policy,
    run_guarded,
    run_retries_only,  # fmt: skip
)
from sentinel.settings import get_settings

EXP_DB = "warehouse_exp"
SYNTH = Path("data/fixtures/synthetic")
RESULTS = Path("experiments/results")


@dataclass
class TrialResult:
    """One row of results.csv."""

    fault: str
    mode: str
    seed: int
    target: str
    outputs_correct: bool
    recovered_verified: bool
    escalated: bool
    escalation_expected: bool
    handled_as_expected: bool
    n_incidents: int
    failure_classes: str
    expected_class: str
    classified_correctly: bool | None
    actions: str
    n_actions: int
    inappropriate_actions: int
    time_to_verified_recovery_s: float | None
    wall_s: float
    fact_measurement_hourly_missing: int
    fact_measurement_hourly_extra: int
    fact_measurement_hourly_wrong: int
    mart_daily_location_missing: int
    mart_daily_location_extra: int
    mart_daily_location_wrong: int
    mart_exceedance_summary_missing: int
    mart_exceedance_summary_extra: int
    mart_exceedance_summary_wrong: int


def connect_exp() -> Conn:
    """Connection to the experiment database (never the live warehouse)."""
    return psycopg.connect(get_settings().conninfo(dbname=EXP_DB), autocommit=True)


def clean_dataset(root: Path = SYNTH) -> dict[str, bytes]:
    """The clean starting files of every trial."""
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in sorted(root.rglob("*.csv.gz"))}


def _prepare(conn: Conn, locations_csv: Path) -> None:
    reset_database(conn)
    load_guidelines(conn)
    seed_locations(conn, locations_csv)


def run_trial(
    fault: str,
    mode: str,
    seed: int,
    clean: dict[str, bytes],
    run0: datetime,
    backoff_scale: float,
    workdir: Path,
    locations_csv: Path = SYNTH / "LOCATIONS.csv",
) -> TrialResult:
    """Run one trial end to end and measure it against golden."""
    trial = build_trial(fault, seed, clean)
    landing, raw = workdir / "landing", workdir / "raw"
    shutil.rmtree(workdir, ignore_errors=True)
    landing.mkdir(parents=True)
    clock = {"now": run0}
    pipe = Pipeline(
        RawStore(raw), load_quality_config(), ("pm25", "pm10"), now=lambda: clock["now"]
    )
    conn = connect_exp()
    _prepare(conn, locations_csv)
    faults_hooks.disarm_all()
    expected = trial.expected_days
    engine = Engine(
        pipe,
        load_policy(),
        DictSource(trial.source_at[0]),
        landing,
        connect_exp,
        origin="injected",
        sleep=lambda s: None,
        backoff_scale=backoff_scale,
    )
    retries = 0
    first_error_at: float | None = None
    clean_after_error_at: float | None = None
    t0 = time.perf_counter()
    try:
        for r in range(N_RUNS):
            clock["now"] = run0 + timedelta(days=r)
            for key, data in sorted(trial.deliveries[r].items()):
                write_atomic(landing / key, data)
            if trial.crash and trial.crash[0] == r:
                faults_hooks.arm(trial.crash[1], trial.crash[2])
            if mode == "guarded":
                engine.source = DictSource(trial.source_at[r])
                engine.current_run = r
                conn = run_guarded(engine, conn, expected, clock["now"])
            elif mode == "retries_only":
                before = time.perf_counter()
                conn, n_retry, n_err = run_retries_only(
                    pipe,
                    conn,
                    connect_exp,
                    landing,
                    sorted({loc for loc, _ in expected}),
                    sleep=lambda s: None,
                )
                retries += n_retry
                if n_err and first_error_at is None:
                    first_error_at = before
                if n_err and n_retry and clean_after_error_at is None:
                    clean_after_error_at = time.perf_counter()
            else:
                raise ValueError(f"mode {mode} is not automated (manual trials: RUNBOOK)")
    finally:
        faults_hooks.disarm_all()
    wall = time.perf_counter() - t0
    if conn.closed or conn.broken:
        conn = connect_exp()
    elif conn.info.transaction_status != 0:
        conn.rollback()
    rebuild_golden(conn, pipe, trial.truth)
    diffs = compare_all(conn)
    dmg = damage(diffs)
    correct = all(d.equal for d in diffs)
    incidents = engine.closed + list(engine.open.values())
    conn.close()
    return _result(
        trial, mode, correct, dmg, wall, incidents, retries, first_error_at, clean_after_error_at
    )


def _result(
    trial: Trial,
    mode: str,
    correct: bool,
    dmg: dict[str, int],
    wall: float,
    incidents: list[Any],
    retries: int,
    first_err: float | None,
    clean_after: float | None,
) -> TrialResult:
    exp = trial.expected
    classes = sorted({i.failure_class for i in incidents})
    actions = [a for i in incidents for a in i.actions]
    escalated = any(i.escalated for i in incidents)
    if mode == "guarded":
        inappropriate = sum(
            len(i.actions)
            for i in incidents
            if not exp.automatic or i.failure_class != exp.failure_class
        )
        classified = (classes == [exp.failure_class]) if exp.failure_class else (classes == [])
        recovered = [
            i.resolved_at - i.detected_at
            for i in incidents
            if i.verified and i.resolved_at is not None
        ]
        ttr = max(recovered) if recovered else (0.0 if not incidents else None)
    else:
        transient = exp.failure_class in {"transient_db", "process_crash"}
        inappropriate = 0 if transient else retries
        actions = ["rerun"] * retries
        classified = None
        ttr = (
            (clean_after - first_err)
            if (first_err and clean_after)
            else (0.0 if first_err is None else None)
        )
    recovered_verified = correct and not escalated
    handled = correct and escalated == exp.escalate
    if not correct:
        ttr = None
    return TrialResult(
        trial.fault,
        mode,
        trial.seed,
        trial.target,
        correct,
        recovered_verified,
        escalated,
        exp.escalate,
        handled,
        len(incidents),
        ";".join(classes),
        exp.failure_class or "",
        classified,
        ";".join(actions),
        len(actions),
        inappropriate,
        None if ttr is None else round(ttr, 4),
        round(wall, 4),
        **dmg,
    )


def environment() -> dict[str, Any]:
    """Hardware, software and commit recorded with every result (rule 9)."""
    files = ["config/policy.yaml", "config/quality_rules.yaml", "config/experiments.yaml",
             "config/guidelines.yaml", "sql/marts/refresh_daily.sql"]  # fmt: skip
    with db.connect() as c:
        server = c.execute("SHOW server_version").fetchone()
    return {
        "utc": datetime.now().astimezone().isoformat(timespec="seconds"),
        "git_commit": git_commit(),
        "working_tree_clean": "not checked",
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "cpu_count": __import__("os").cpu_count(),
        "postgres": server[0] if server else None,
        "packages": {p: version(p) for p in ("sentinel-aq", "polars", "psycopg", "pandera")},
        "config_sha256": {f: sha256_hex(Path(f).read_bytes()) for f in files},
        "database": EXP_DB,
    }


def run_all(
    cfg_path: Path = Path("config/experiments.yaml"), progress: Callable[[str], None] = print
) -> Path:
    """Every fault x mode x seed from the config. Writes results.csv and metadata.json."""
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    if cfg["dataset"] != "synthetic":
        raise NotImplementedError("real-data trials need the M1 location choice first")
    with connect_exp() as c:
        db.migrate(c)
    clean = clean_dataset()
    run0 = datetime.fromisoformat(cfg["run0_utc"])
    rows: list[dict[str, Any]] = []
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="sentinel-exp-") as tmp:
        for fault in cfg["faults"]:
            for mode in cfg["modes"]:
                for seed in cfg["seeds"]:
                    res = run_trial(
                        fault,
                        mode,
                        seed,
                        clean,
                        run0,
                        float(cfg["backoff_scale"]),
                        Path(tmp) / f"{fault}-{mode}-{seed}",
                    )
                    rows.append(asdict(res))
                    progress(
                        f"{fault} {mode} seed={seed} correct={res.outputs_correct} "
                        f"escalated={res.escalated} classes={res.failure_classes}"
                    )
    df = pl.DataFrame(rows, infer_schema_length=None)
    base = (
        df.filter(pl.col("fault") == "F0")
        .group_by("mode")
        .agg(pl.col("wall_s").mean().alias("_base"))
    )
    df = (
        df.join(base, on="mode", how="left")
        .with_columns((pl.col("wall_s") - pl.col("_base")).round(4).alias("overhead_s"))
        .drop("_base")
    )
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / "results.csv"
    df.write_csv(out)
    meta = environment() | {
        "experiment_config": cfg,
        "n_trials": df.height,
        "total_wall_s": round(time.perf_counter() - started, 1),
        "manual_mode": "not run: needs a human operator (make manual-trial)",
    }
    (RESULTS / "metadata.json").write_text(
        json.dumps(meta, indent=2, default=str) + "\n", encoding="utf-8"
    )
    return out


def iqr(xs: list[float]) -> tuple[float, float]:
    """(Q1, Q3) with the 'inclusive' method; for tiny samples the min/max."""
    if len(xs) < 2:
        return (xs[0], xs[0]) if xs else (float("nan"), float("nan"))
    q = statistics.quantiles(xs, n=4, method="inclusive")
    return q[0], q[2]
