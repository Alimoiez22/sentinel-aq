"""Command-line entry point: `sentinel <command>`.

Airflow tasks (M8) and the Makefile call these commands, so every pipeline step can
also be run by hand. That is what the manual recovery mode in the experiments needs.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg

from sentinel import __version__, commands, m1
from sentinel.config import load_scope
from sentinel.http import HttpClient
from sentinel.log import configure_logging, get_logger
from sentinel.openaq.api import OpenAQClient
from sentinel.settings import get_settings


def check_db() -> int:
    """Connect to the warehouse and report server and PostGIS versions as JSON."""
    settings = get_settings()
    log = get_logger(command="check-db")
    with psycopg.connect(settings.conninfo(), connect_timeout=10) as conn:
        row = conn.execute(
            "SELECT current_setting('server_version'), postgis_lib_version()"
        ).fetchone()
    if row is None:  # pragma: no cover - SELECT without FROM always returns a row
        log.error("no_row_returned")
        return 1
    result = {"server_version": row[0], "postgis_version": row[1]}
    log.info("db_ok", **result)
    print(json.dumps(result))
    return 0


def _ids(text: str) -> list[int]:
    return [int(x) for x in text.replace(",", " ").split()]


def build_parser() -> argparse.ArgumentParser:
    """All subcommands and their options."""
    parser = argparse.ArgumentParser(prog="sentinel", description=__doc__)
    parser.add_argument("--version", action="version", version=f"sentinel {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-db", help="connect to the warehouse and print versions")
    dc = sub.add_parser(
        "discover", help="list OpenAQ locations of the scope country (needs API key)"
    )
    dc.add_argument("--from-csv", type=Path, help="no API: locations from a provenance CSV")
    cl = sub.add_parser("coverage-listing", help="S3 listings for candidate locations")
    cl.add_argument("--lahore-only", action="store_true", help="only locations within the radius")
    fe = sub.add_parser("fetch", help="download listed files of chosen locations")
    fe.add_argument("--locations", type=_ids, required=True, help="ids, comma or space separated")
    fe.add_argument("--confirm", action="store_true", help="without it, only print the plan")
    sub.add_parser("coverage-hours", help="hours per day from fetched files")
    sub.add_parser("coverage-report", help="summary CSV and Markdown table")
    sub.add_parser("snapshot", help="daily archive snapshot of scope.snapshot_locations")
    commands.add_parsers(sub)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run one command. Returns the process exit code."""
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    if args.command == "check-db":
        return check_db()

    scope, paths = load_scope(), m1.Paths()
    if args.command == "discover" and args.from_csv:
        print(m1.discover_from_csv(scope, args.from_csv, paths))
        return 0
    if args.command == "discover":
        key = settings.openaq_api_key.get_secret_value()
        if not key:
            print("OPENAQ_API_KEY is empty in .env; register a free key at openaq.org first.")
            return 2
        print(m1.discover(scope, OpenAQClient(key, HttpClient()), paths))
    elif args.command == "coverage-listing":
        print(m1.coverage_listing(scope, m1.archive_client(), paths, args.lahore_only))
    elif args.command == "fetch":
        plan = m1.fetch_plan(paths, args.locations)
        print(json.dumps(plan))
        if not args.confirm:
            print("plan only; rerun with --confirm (make fetch ... CONFIRM=yes) to download")
            return 0
        print(json.dumps({"files_stored": m1.fetch(m1.archive_client(), paths, args.locations)}))
    elif args.command == "coverage-hours":
        print(m1.coverage_hours(paths))
    elif args.command == "coverage-report":
        print(m1.coverage_report(scope, paths))
    elif args.command == "snapshot":
        print(m1.snapshot(scope, m1.archive_client(), paths))
    else:
        return commands.handle(args, scope)
    return 0


if __name__ == "__main__":
    sys.exit(main())
