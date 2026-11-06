"""The Compose database is the version we pinned and is set up as designed (ADR-002)."""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest

from sentinel.cli import main
from sentinel.settings import get_settings

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def conn() -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    with psycopg.connect(get_settings().conninfo(), connect_timeout=10) as c:
        yield c


def _one(conn: psycopg.Connection[tuple[object, ...]], sql: str) -> object:
    row = conn.execute(sql).fetchone()
    assert row is not None
    return row[0]


def test_postgres_major_version_is_18(conn: psycopg.Connection[tuple[object, ...]]) -> None:
    num = int(str(_one(conn, "SHOW server_version_num")))
    assert 180000 <= num < 190000


def test_postgis_installed_in_warehouse(conn: psycopg.Connection[tuple[object, ...]]) -> None:
    version = str(_one(conn, "SELECT extversion FROM pg_extension WHERE extname = 'postgis'"))
    assert version.startswith("3.6")


def test_airflow_metadata_is_a_separate_database(
    conn: psycopg.Connection[tuple[object, ...]],
) -> None:
    owner = _one(
        conn,
        "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = 'airflow_meta'",
    )
    assert owner == "airflow"


def test_warehouse_role_cannot_read_airflow_db(
    conn: psycopg.Connection[tuple[object, ...]],
) -> None:
    allowed = _one(conn, "SELECT has_database_privilege('airflow_meta', 'CONNECT')")
    assert allowed is False


def test_cli_check_db(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["check-db"]) == 0
    assert '"postgis_version"' in capsys.readouterr().out
