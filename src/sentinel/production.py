"""Production entry points (M8): the daily guarded run, revision watch and backfill.

These are what the Airflow DAGs call. They use the real archive as the recovery source:
a refetch or backfill downloads the file again from OpenAQ (anonymous HTTPS, ADR-010).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sentinel import db
from sentinel.config import Scope
from sentinel.http import HttpClient, HttpError
from sentinel.openaq.archive import Archive
from sentinel.ops import run_landing
from sentinel.pipeline import Pipeline
from sentinel.recovery import Engine, load_policy, run_guarded

LANDING = Path("data/landing_drop")
REVISION_WINDOW_DAYS = 14  # PROJECT_BRIEF §5: re-scan the last N days for revisions


@dataclass
class ArchiveSource:
    """Recovery source backed by the real OpenAQ archive. 404 -> None (not published)."""

    archive: Archive

    def get(self, key: str) -> bytes | None:
        try:
            return self.archive.download(key)
        except HttpError as exc:
            if exc.status in (403, 404):  # S3 answers 403 for a missing key without list rights
                return None
            raise


def expected_days(scope: Scope, start: date, end: date) -> set[tuple[int, date]]:
    """Every (location, day) the scope promises to publish in [start, end]."""
    days = (end - start).days + 1
    return {(loc, start + timedelta(days=i)) for loc in scope.locations for i in range(days)}


def daily(
    scope: Scope,
    pipe: Pipeline,
    start: date | None = None,
    end: date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Guarded run over [start, end] (default: the last REVISION_WINDOW_DAYS days up to the
    scope end). Records a run in ops.run_log and returns a JSON-able summary."""
    now = now or datetime.now(UTC)
    end = end or scope.date_range.resolved_end(now.date())
    start = start or max(scope.date_range.start, end - timedelta(days=REVISION_WINDOW_DAYS - 1))
    conn = db.connect()
    run_id = _start_run(conn, "guarded", f"daily {start}..{end}")
    engine = Engine(
        pipe,
        load_policy(),
        ArchiveSource(Archive(HttpClient())),
        LANDING,
        db.connect,
        origin="real",
        run_id=run_id,
    )
    conn = run_guarded(engine, conn, expected_days(scope, start, end), now)
    incidents = engine.closed + list(engine.open.values())
    summary = {
        "run_id": run_id, "start": str(start), "end": str(end),
        "incidents": len(incidents),
        "escalated": sum(i.escalated for i in incidents),
        "recovered": sum(i.verified and not i.escalated for i in incidents),
        "open": len(engine.open),
    }  # fmt: skip
    _end_run(conn, run_id, "escalations" if summary["escalated"] else "ok", summary)
    conn.close()
    return summary


def backfill(scope: Scope, pipe: Pipeline, start: date, end: date) -> dict[str, Any]:
    """Process landing files for an explicit date range (no recovery actions)."""
    with db.connect() as conn:
        run_id = _start_run(conn, "plain", f"backfill {start}..{end}")
        s = run_landing(conn, pipe, LANDING, scope.locations, start, end, raise_errors=False)
        out = {"run_id": run_id, "registered": s.registered, "processed": len(s.processed),
               "errors": len(s.errors)}  # fmt: skip
        _end_run(conn, run_id, "errors" if s.errors else "ok", out)
    return out


def _start_run(conn: Any, mode: str, command: str) -> int:
    from sentinel.provenance import git_commit

    row = conn.execute(
        "INSERT INTO ops.run_log (mode, command, git_commit) VALUES (%s,%s,%s) RETURNING run_id",
        (mode, command, git_commit()),
    ).fetchone()
    return int(row[0])


def _end_run(conn: Any, run_id: int, outcome: str, detail: dict[str, Any]) -> None:
    from psycopg.types.json import Jsonb

    if conn.closed or conn.broken:
        conn = db.connect()
    conn.execute("UPDATE ops.run_log SET ended_at = now(), outcome = %s, detail = %s"
                 " WHERE run_id = %s", (outcome, Jsonb(json.loads(json.dumps(detail, default=str))),
                                        run_id))  # fmt: skip
