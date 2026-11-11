"""Manual-mode trials (Track A, mode = manual). A HUMAN recovers; this only measures.

Protocol (docs/RUNBOOK.md, "Running a manual trial"):
1. `make manual-start FAULT=F2 SEED=11 OPERATOR=<name>`: prepares warehouse_exp, injects
   the fault, runs both scheduled runs WITHOUT any recovery (the pipeline halts), saves
   the upstream source files to data/manual_trial/source/, prints the problem summary,
   and starts the clock.
2. The operator diagnoses and repairs using only the RUNBOOK and the CLI.
3. `make manual-finish`: stops the clock, builds golden from the trial's truth, compares,
   and appends one row to experiments/results/manual_trials.csv (operator, trial number).
Nothing here is ever written to results.csv: manual numbers are labelled human-operated.
"""

from __future__ import annotations

import csv
import json
import shutil
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sentinel import db, faults_hooks
from sentinel.experiments import SYNTH, _prepare, clean_dataset, connect_exp
from sentinel.faults import N_RUNS, build_trial
from sentinel.ops import run_landing
from sentinel.pipeline import Pipeline
from sentinel.quality import load_quality_config
from sentinel.rawstore import RawStore, write_atomic
from sentinel.reconcile import compare_all, damage, rebuild_golden

WORK = Path("data/manual_trial")
STATE = WORK / "state.json"
OUT = Path("experiments/results/manual_trials.csv")
RUN0 = datetime.fromisoformat("2025-11-07T00:00:00+00:00")


def _pipe() -> Pipeline:
    return Pipeline(
        RawStore(WORK / "raw"),
        load_quality_config(),
        ("pm25", "pm10"),
        now=lambda: RUN0 + timedelta(days=N_RUNS - 1),
    )


def start(fault: str, seed: int, operator: str) -> dict[str, Any]:
    """Set up the trial and start the clock."""
    if STATE.exists():
        raise SystemExit("a manual trial is already running: `make manual-finish` first")
    trial = build_trial(fault, seed, clean_dataset())
    shutil.rmtree(WORK, ignore_errors=True)
    for key, data in trial.source_at[-1].items():
        write_atomic(WORK / "source" / key, data)
    conn = connect_exp()
    with conn:
        db.migrate(conn)
    conn = connect_exp()
    _prepare(conn, SYNTH / "LOCATIONS.csv")
    pipe = _pipe()
    errors: list[dict[str, Any]] = []
    for r in range(N_RUNS):
        for key, data in trial.deliveries[r].items():
            write_atomic(WORK / "landing" / key, data)
        if trial.crash and trial.crash[0] == r:
            faults_hooks.arm(trial.crash[1], trial.crash[2])
        if conn.closed or conn.broken:
            conn = connect_exp()
        errors += run_landing(
            conn, pipe, WORK / "landing", [99000001, 99000002], raise_errors=False
        ).errors
    faults_hooks.disarm_all()
    state = {"fault": fault, "seed": seed, "operator": operator, "started": time.time(),
             "started_utc": datetime.now(UTC).isoformat(timespec="seconds"),
             "run_errors": errors}  # fmt: skip
    STATE.write_text(json.dumps(state, indent=1, default=str), encoding="utf-8")
    return {
        "started": state["started_utc"],
        "run_errors": len(errors),
        "landing": str(WORK / "landing"),
        "source": str(WORK / "source"),
        "database": "warehouse_exp",
        "next": "follow docs/RUNBOOK.md, then make manual-finish",
    }


def finish() -> dict[str, Any]:
    """Stop the clock, compare with golden, append to manual_trials.csv."""
    if not STATE.exists():
        raise SystemExit("no manual trial is running")
    state = json.loads(STATE.read_text(encoding="utf-8"))
    elapsed = time.time() - float(state["started"])
    trial = build_trial(state["fault"], int(state["seed"]), clean_dataset())
    with connect_exp() as conn:
        rebuild_golden(conn, _pipe(), trial.truth)
        diffs = compare_all(conn)
    prior = 0
    if OUT.exists():
        with OUT.open(encoding="utf-8") as f:
            prior = sum(1 for r in csv.DictReader(f) if r["operator"] == state["operator"])
    row = {"fault": state["fault"], "seed": state["seed"], "mode": "manual",
           "operator": state["operator"], "operator_trial_no": prior + 1,
           "started_utc": state["started_utc"], "elapsed_s": round(elapsed, 1),
           "outputs_correct": all(d.equal for d in diffs), **damage(diffs),
           "label": "human-operated"}  # fmt: skip
    new = not OUT.exists()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row), lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(row)
    STATE.unlink()
    return row
