"""Integration fixtures: a migrated, freshly reset `warehouse_test` per test."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from sentinel import db, faults_hooks
from sentinel.ops import load_guidelines, reset_database, seed_locations
from sentinel.pipeline import Pipeline
from sentinel.quality import load_quality_config
from sentinel.rawstore import RawStore
from sentinel.settings import get_settings

ROOT = Path(__file__).resolve().parents[2]
SYNTH = ROOT / "data/fixtures/synthetic"
NOW = datetime(2026, 1, 1, tzinfo=UTC)  # fixed processing time: results do not drift


def connect_test() -> psycopg.Connection[tuple[object, ...]]:
    return psycopg.connect(get_settings().conninfo(dbname="warehouse_test"))


@pytest.fixture(scope="session")
def migrated() -> None:
    with connect_test() as conn:
        db.migrate(conn, ROOT / "sql/ddl")


@pytest.fixture
def conn(migrated: None) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    faults_hooks.disarm_all()
    c = connect_test()
    reset_database(c)
    load_guidelines(c, ROOT / "config/guidelines.yaml")
    seed_locations(c, SYNTH / "LOCATIONS.csv")
    yield c
    faults_hooks.disarm_all()
    c.close()


@pytest.fixture
def pipe(tmp_path: Path) -> Pipeline:
    return Pipeline(
        RawStore(tmp_path / "raw"),
        load_quality_config(ROOT / "config/quality_rules.yaml"),
        ("pm25", "pm10"),
        now=lambda: NOW,
    )


def synthetic_files() -> list[tuple[str, bytes]]:
    """(archive key, bytes) of every synthetic fixture file, in key order."""
    return [
        (p.relative_to(SYNTH).as_posix(), p.read_bytes()) for p in sorted(SYNTH.rglob("*.csv.gz"))
    ]
