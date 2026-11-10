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
    sub.add_parser("golden", help="rebuild the golden schema from the latest revisions")
    sub.add_parser("reconcile", help="compare published tables with golden (exit 1 if different)")
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
            summary = run_landing(
                conn,
                make_pipeline(scope),
                args.landing,
                ids,
                args.start,
                args.end,
                raise_errors=False,
            )
            print(json.dumps(asdict(summary), default=str))
            return 1 if summary.errors else 0
        elif args.command == "golden":
            pipe = make_pipeline(scope)
            print(
                json.dumps(
                    {"location_days": rebuild_golden(conn, pipe, manifest_inputs(conn, pipe))}
                )
            )
        elif args.command == "reconcile":
            diffs = compare_all(conn)
            print(json.dumps([asdict(d) | {"equal": d.equal} for d in diffs], default=str))
            return 0 if all(d.equal for d in diffs) else 1
    return 0


def _connect_to(dbname: str):  # type: ignore[no-untyped-def]
    import psycopg

    from sentinel.settings import get_settings

    return psycopg.connect(get_settings().conninfo(dbname=dbname), autocommit=True)
