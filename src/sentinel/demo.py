"""`make demo` (PROJECT_BRIEF §17): a 3-day run with an injected fault AND a revision.

Scenario on warehouse_exp, synthetic data, guarded mode:
* run 0: all six location-days arrive, but one file is truncated in transit (F2);
* run 1: a retroactive revision of another day arrives (F9).
Prints incident -> decision -> action -> verification, then the golden comparison.
"""

from __future__ import annotations

import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Any

from sentinel import db
from sentinel.experiments import SYNTH, _prepare, clean_dataset, connect_exp
from sentinel.faults import N_RUNS, build_trial, truncate
from sentinel.manual import RUN0
from sentinel.openaq.archive import parse_key
from sentinel.pipeline import Pipeline
from sentinel.quality import load_quality_config
from sentinel.rawstore import RawStore, write_atomic
from sentinel.reconcile import compare_all, rebuild_golden
from sentinel.recovery import DictSource, Engine, load_policy, run_guarded


def run(seed: int = 11) -> dict[str, Any]:
    """Run the demo scenario and return what happened."""
    import random

    clean = clean_dataset()
    trial = build_trial("F9", seed, clean)  # revision of trial.target at run 1
    other = sorted(k for k in clean if k != trial.target)[0]
    trial.deliveries[0][other] = truncate(clean[other], random.Random(seed))  # noqa: S311
    with connect_exp() as c:
        db.migrate(c)
    conn = connect_exp()
    _prepare(conn, SYNTH / "LOCATIONS.csv")
    with tempfile.TemporaryDirectory() as tmp:
        landing = Path(tmp) / "landing"
        clock = {"now": RUN0}
        pipe = Pipeline(
            RawStore(Path(tmp) / "raw"),
            load_quality_config(),
            ("pm25", "pm10"),
            now=lambda: clock["now"],
        )
        engine = Engine(
            pipe,
            load_policy(),
            DictSource(trial.source_at[0]),
            landing,
            connect_exp,
            origin="injected",
        )
        for r in range(N_RUNS):
            clock["now"] = RUN0 + timedelta(days=r)
            for key, data in trial.deliveries[r].items():
                write_atomic(landing / key, data)
            engine.source, engine.current_run = DictSource(trial.source_at[r]), r
            conn = run_guarded(engine, conn, trial.expected_days, clock["now"])
        if conn.closed or conn.broken:
            conn = connect_exp()
        incidents = conn.execute(
            "SELECT incident_id, location_id, source_date, failure_class, decision, policy_rule_id,"
            " action, outcome, verified, escalated, round(time_to_verified_recovery_s::numeric, 3)"
            " FROM ops.incident ORDER BY incident_id"
        ).fetchall()
        actions = conn.execute(
            "SELECT incident_id, action, result FROM ops.action_log ORDER BY action_id"
        ).fetchall()
        revised = conn.execute(
            "SELECT location_id, local_date, parameter, publication_status"
            " FROM wh.mart_daily_location WHERE publication_status = 'revised'"
            " ORDER BY 1, 2, 3"
        ).fetchall()
        rebuild_golden(conn, pipe, trial.truth)
        diffs = compare_all(conn)
    conn.close()
    return {
        "fault_key": other,
        "revision_key": trial.target,
        "fault_day": parse_key(other),
        "revision_day": parse_key(trial.target),
        "incidents": incidents,
        "actions": actions,
        "revised_outputs": revised,
        "golden_equal": all(d.equal for d in diffs),
        "diffs": [(d.table, d.missing, d.extra, d.wrong, d.checksum_equal) for d in diffs],
    }


def print_report(out: dict[str, Any]) -> None:
    """Human-readable demo output."""
    print(f"Injected: truncated transfer of {out['fault_day']} ; revision of {out['revision_day']}")
    print(
        "\nIncidents (id, location, day, class, decision, rule, action, outcome, verified,"
        " escalated, time_to_verified_recovery_s):"
    )
    for r in out["incidents"]:
        print("  ", r)
    print("\nAudit log (incident, action, result):")
    for r in out["actions"]:
        print("  ", r)
    print("\nOutputs marked revised:", out["revised_outputs"])
    print("\nPublished vs golden (table, missing, extra, wrong, checksum_equal):")
    for d in out["diffs"]:
        print("  ", d)
    print(
        "\nRESULT:",
        "verified recovery, outputs equal golden"
        if out["golden_equal"]
        else "OUTPUTS DIFFER FROM GOLDEN",
    )
