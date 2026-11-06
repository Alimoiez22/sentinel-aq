"""The synthetic fixtures are reproducible and cover both header variants."""

from __future__ import annotations

import csv
import gzip
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "data" / "fixtures" / "synthetic"


def _load_generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "make_fixtures", ROOT / "scripts/make_fixtures.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["make_fixtures"] = module  # dataclasses need the module registered
    spec.loader.exec_module(module)
    return module


gen = _load_generator()


def _committed_manifest() -> dict[str, str]:
    with (FIXTURES / "MANIFEST.csv").open(encoding="utf-8") as f:
        return {row["path"]: row["sha256"] for row in csv.DictReader(f)}


def _header(path: Path) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return next(csv.reader(f))


def test_regeneration_is_byte_identical(tmp_path: Path) -> None:
    assert gen.generate(tmp_path) == _committed_manifest()


def test_committed_files_match_manifest() -> None:
    import hashlib

    for rel, digest in _committed_manifest().items():
        assert hashlib.sha256((FIXTURES / rel).read_bytes()).hexdigest() == digest, rel


def test_both_header_variants_present() -> None:
    headers = {tuple(_header(FIXTURES / rel)) for rel in _committed_manifest()}
    assert headers == {tuple(gen.HEADER_A), tuple(gen.HEADER_B)}


@pytest.mark.parametrize("rel", sorted(_committed_manifest()))
def test_each_file_parses_with_polars(rel: str) -> None:
    df = pl.read_csv(FIXTURES / rel)
    assert df.height == 48  # 2 sensors x 24 hours
    assert set(df["parameter"].unique()) == {"pm25", "pm10"}
    stamps = df["datetime"].str.to_datetime("%Y-%m-%dT%H:%M:%S%:z")
    assert stamps.null_count() == 0
    assert (df["value"] >= 0).all()


def test_paths_follow_archive_layout() -> None:
    for rel in _committed_manifest():
        parts = Path(rel).parts
        assert parts[:2] == ("records", "csv.gz")
        assert parts[2].startswith("locationid=")
        assert parts[3].startswith("year=") and parts[4].startswith("month=")
        assert len(parts[4]) == len("month=MM")
