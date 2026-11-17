"""CLI handlers for the pipeline commands (M2-M7). Kept apart from cli.py to keep it small."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date
from pathlib import Path

from sentinel import db
from sentinel.config import Scope
from sentinel.ops import load_guidelines, run_landing, seed_locations
from sentinel.pipeline import Pipeline
from sentinel.quality import load_quality_config
from sentinel.rawstore import RawStore
from sentinel.reconcile import compare_all, manifest_inputs, rebuild_golden


def make_pipeline(scope: Scope, data_dir: Path = Path("data")) -> Pipeline:
    """The standard pipeline: raw store under data/raw, rules from config/."""
    return Pipeline(RawStore(data_dir / "raw"), load_quality_config(), tuple(scope.parameters))


def add_parsers(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the pipeline subcommands."""
    m = sub.add_parser("migrate", help="apply sql/ddl migrations and load guidelines")
    m.add_argument("--db", help="database name (default: SENTINEL_DB_NAME)")
    s = sub.add_parser("seed-locations", help="upsert wh.dim_location from a locations CSV")
    s.add_argument("--csv", type=Path, default=Path("results/m1_locations.csv"))
    s.add_argument("--locations", help="ids to include (default: scope.locations)")
    r = sub.add_parser("run", help="register landing files and process them")
    r.add_argument("--locations", help="ids (default: scope.locations)")
    r.add_argument("--start", type=date.fromisoformat)
    r.add_argument("--end", type=date.fromisoformat)
    r.add_argument("--landing", type=Path, default=Path("data/landing_drop"))
    r.add_argument("--raw", type=Path, default=Path("data/raw"), help="raw store (manual trials)")
    sub.add_parser("revisions-report", help="Track B report from snapshots + live warehouse")
    an = sub.add_parser("analyse", help="summaries and figures from experiment results")
    an.add_argument("--dir", type=Path, default=Path("experiments/results"))
    an.add_argument("--tag", default="", help='"" synthetic, "real" for the Lahore window')
    dash = sub.add_parser("dashboard", help="static HTML dashboard over the ops views")
    dash.add_argument("--db", default=None, help="database (default: SENTINEL_DB_NAME)")
    dash.add_argument("--out", type=Path, default=Path("docs/dashboard.html"))
    ms = sub.add_parser("manual-start", help="set up a manual-mode trial and start the clock")
    ms.add_argument("--fault", required=True)
    ms.add_argument("--seed", type=int, required=True)
    ms.add_argument("--operator", required=True)
    sub.add_parser("manual-finish", help="stop the clock, compare with golden, record")
    sub.add_parser("demo", help="3-day run with an injected fault and a revision")
    sub.add_parser("golden", help="rebuild the golden schema from the latest revisions")
    rc = sub.add_parser("rules-compare", help="blocked days under two quality-rule versions")
    rc.add_argument("--old", type=Path, default=Path("config/archive/quality_rules_v1.yaml"))
    sub.add_parser("lahore-report", help="Lahore PM2.5 by month and winter, from the marts")
    rp = sub.add_parser("reprocess", help="replay quality..verify after a rule change")
    rp.add_argument("--locations", help="ids (default: scope.locations)")
    sub.add_parser("reconcile", help="compare published tables with golden (exit 1 if different)")
    d = sub.add_parser("daily", help="guarded run over the last 14 days of the scope")
    d.add_argument("--start", type=date.fromisoformat)
    d.add_argument("--end", type=date.fromisoformat)
    b = sub.add_parser("backfill", help="process landing files for a date range (no actions)")
    b.add_argument("--start", type=date.fromisoformat, required=True)
    b.add_argument("--end", type=date.fromisoformat, required=True)
    e = sub.add_parser("experiment", help="Track A: faults x modes x seeds -> results.csv")
    e.add_argument("--config", type=Path, default=Path("config/experiments.yaml"))
    t = sub.add_parser("trial", help="run one trial and print its result (debugging)")
    t.add_argument("--fault", required=True)
    t.add_argument("--mode", default="guarded")
    t.add_argument("--seed", type=int, default=1)


def _ids(text: str | None, default: list[int]) -> list[int]:
    return [int(x) for x in text.replace(",", " ").split()] if text else list(default)


def handle(args: argparse.Namespace, scope: Scope) -> int:
    """Run one pipeline command."""
    if args.command == "rules-compare":
        from sentinel.quality import load_quality_config
        from sentinel.rulecompare import compare

        outs = compare(
            load_quality_config(args.old),
            load_quality_config(),
            Path("data/landing_drop"),
            Path("results/m1_locations.csv"),
            scope.locations,
            tuple(scope.parameters),
        )
        print(json.dumps([str(p) for p in outs]))
        return 0
    if args.command == "analyse":
        from sentinel.analysis import analyse

        print(json.dumps([str(p) for p in analyse(args.dir, args.tag)]))
        return 0
    if args.command == "manual-start":
        from sentinel.manual import start

        print(json.dumps(start(args.fault, args.seed, args.operator), indent=1))
        return 0
    if args.command == "manual-finish":
        from sentinel.manual import finish

        print(json.dumps(finish(), indent=1, default=str))
        return 0
    if args.command == "demo":
        from sentinel.demo import print_report, run

        out = run()
        print_report(out)
        return 0 if out["golden_equal"] else 1
    if args.command == "dashboard":
        from sentinel.dashboard import write

        with _connect_to(args.db) if args.db else db.connect() as conn:
            print(write(conn, args.out))
        return 0
    if args.command in ("daily", "backfill"):
        from sentinel import production

        if not scope.locations:
            print("config/scope.yaml has no locations yet (choose them after M1)")
            return 2
        pipe = make_pipeline(scope)
        out = (
            production.daily(scope, pipe, args.start, args.end)
            if args.command == "daily"
            else production.backfill(scope, pipe, args.start, args.end)
        )
        print(json.dumps(out, default=str))
        return 0 if not out.get("errors") and not out.get("escalated") else 1
    if args.command == "experiment":
        from sentinel.experiments import run_all

        print(run_all(args.config))
        return 0
    if args.command == "trial":
        import tempfile
        from datetime import datetime

        from sentinel.experiments import clean_dataset, connect_exp, run_trial

        with connect_exp() as c:
            db.migrate(c)
        with tempfile.TemporaryDirectory() as tmp:
            res = run_trial(
                args.fault,
                args.mode,
                args.seed,
                clean_dataset(),
                datetime.fromisoformat("2025-11-07T00:00:00+00:00"),
                0.0,
                Path(tmp),
            )
        print(json.dumps(asdict(res), default=str, indent=1))
        return 0
    if args.command == "migrate":
        with db.connect() if not args.db else _connect_to(args.db) as conn:
            applied = db.migrate(conn)
            n = load_guidelines(conn)
        print(json.dumps({"applied": applied, "guidelines": n}))
        return 0
    with db.connect() as conn:
        if args.command == "seed-locations":
            ids = _ids(args.locations, scope.locations) or None
            print(json.dumps({"seeded": seed_locations(conn, args.csv, ids)}))
        elif args.command == "run":
            ids = _ids(args.locations, scope.locations)
            if not ids:
                print("no locations: set config/scope.yaml locations or pass --locations")
                return 2
            pipe = make_pipeline(scope)
            pipe.store = RawStore(args.raw)
            summary = run_landing(
                conn,
                pipe,
                args.landing,
                ids,
                args.start,
                args.end,
                raise_errors=False,
            )
            print(json.dumps(asdict(summary), default=str))
            return 1 if summary.errors else 0
        elif args.command == "lahore-report":
            from sentinel.lahore import report as lahore_report

            print(json.dumps([str(p) for p in lahore_report(conn)]))
        elif args.command == "reprocess":
            from sentinel.ops import reprocess

            ids = _ids(args.locations, scope.locations)
            print(json.dumps(reprocess(conn, make_pipeline(scope), ids)))
        elif args.command == "golden":
            pipe = make_pipeline(scope)
            print(
                json.dumps(
                    {"location_days": rebuild_golden(conn, pipe, manifest_inputs(conn, pipe))}
                )
            )
        elif args.command == "revisions-report":
            from sentinel.trackb import report

            print(report(conn))
        elif args.command == "reconcile":
            diffs = compare_all(conn)
            payload = [asdict(d) | {"equal": d.equal} for d in diffs]
            from sentinel.provenance import record

            evidence = Path("results/reconcile_latest.json")
            evidence.parent.mkdir(parents=True, exist_ok=True)
            evidence.write_text(json.dumps(payload, indent=1, default=str) + "\n", encoding="utf-8")
            record("reconcile", [str(evidence)], {"equal": all(d.equal for d in diffs)})
            print(json.dumps(payload, default=str))
            return 0 if all(d.equal for d in diffs) else 1
    return 0


def _connect_to(dbname: str):  # type: ignore[no-untyped-def]
    import psycopg

    from sentinel.settings import get_settings

    return psycopg.connect(get_settings().conninfo(dbname=dbname), autocommit=True)
