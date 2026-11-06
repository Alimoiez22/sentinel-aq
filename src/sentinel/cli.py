"""Command-line entry point: `sentinel <command>`.

Airflow tasks (M8) and the Makefile call these commands, so every pipeline step can
also be run by hand. That is what the manual recovery mode in the experiments needs.
"""

from __future__ import annotations

import argparse
import json
import sys

import psycopg

from sentinel import __version__
from sentinel.log import configure_logging, get_logger
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


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run one command. Returns the process exit code."""
    parser = argparse.ArgumentParser(prog="sentinel", description=__doc__)
    parser.add_argument("--version", action="version", version=f"sentinel {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-db", help="connect to the warehouse and print versions")
    args = parser.parse_args(argv)

    configure_logging(get_settings().log_level)
    if args.command == "check-db":
        return check_db()
    parser.error(f"unknown command {args.command}")  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())
