"""Database connections and migrations.

Migrations are the numbered files in sql/ddl/, applied in order once each. The SHA-256
of every applied file is recorded; if an applied file is later edited, `migrate` stops
instead of silently diverging (create a new numbered file for changes).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import psycopg

from sentinel.settings import get_settings

DDL_DIR = Path("sql/ddl")
# Every pipeline writer of a location-day takes this lock (M3); readers never need it.
LOCK_NAMESPACE = 7_301


class MigrationChangedError(RuntimeError):
    """An already-applied migration file has different content now."""


def connect(autocommit: bool = True) -> psycopg.Connection[tuple[object, ...]]:
    """New connection as the pipeline role.

    Autocommit is the default ON PURPOSE (ADR-015): each pipeline stage opens its own
    `with conn.transaction()`. On a non-autocommit connection the first plain SELECT
    starts an implicit transaction and every later `conn.transaction()` becomes a mere
    SAVEPOINT inside it, so nothing is committed until the connection commits.
    """
    return psycopg.connect(get_settings().conninfo(), autocommit=autocommit, connect_timeout=10)


def migrate(conn: psycopg.Connection[tuple[object, ...]], ddl_dir: Path = DDL_DIR) -> list[str]:
    """Apply pending migrations in one transaction each. Returns the files applied."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS public.schema_migrations ("
        " filename text PRIMARY KEY, sha256 char(64) NOT NULL,"
        " applied_at timestamptz NOT NULL DEFAULT now())"
    )
    conn.commit()
    applied: dict[str, str] = {
        str(f): str(h)
        for f, h in conn.execute("SELECT filename, sha256 FROM public.schema_migrations").fetchall()
    }
    done: list[str] = []
    for path in sorted(ddl_dir.glob("*.sql")):
        sql = path.read_text(encoding="utf-8")
        digest = hashlib.sha256(sql.encode()).hexdigest()
        if path.name in applied:
            if applied[path.name] != digest:
                raise MigrationChangedError(f"{path.name} changed after it was applied")
            continue
        with conn.transaction():
            conn.execute(sql)  # trusted repository file
            conn.execute(
                "INSERT INTO public.schema_migrations (filename, sha256) VALUES (%s, %s)",
                (path.name, digest),
            )
        done.append(path.name)
    return done


def lock_location_day(
    conn: psycopg.Connection[tuple[object, ...]], location_id: int, source_date_ordinal: int
) -> None:
    """Transaction-level advisory lock on one location-day (released at commit/rollback).

    Two-key form: (namespace, hash of location-day). Session-level locks were rejected
    because they survive a rollback (RESEARCH §7.6, ADR-015).
    """
    key = (location_id * 1_000_003 + source_date_ordinal) % 2_147_483_647
    conn.execute("SELECT pg_advisory_xact_lock(%s, %s)", (LOCK_NAMESPACE, key))


def sql_file(path: str) -> str:
    """Read a SQL file from the repository's sql/ folder."""
    return (Path("sql") / path).read_text(encoding="utf-8")
