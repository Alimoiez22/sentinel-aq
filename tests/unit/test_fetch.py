"""Fetching is idempotent, keeps every version, and rejects truncated downloads."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from sentinel.fetch import Fetcher, FetchIndex, SizeMismatchError
from sentinel.rawstore import RawStore, sha256_hex
from tests.stubs import StubArchive, day_key

KEY = day_key(5, date(2025, 11, 1))


def _fetcher(tmp_path: Path, archive: StubArchive) -> Fetcher:
    return Fetcher(
        archive,
        RawStore(tmp_path / "raw"),
        tmp_path / "landing",
        FetchIndex(tmp_path / "raw" / "index.csv"),
    )


def test_second_fetch_downloads_nothing(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(KEY, b"v1")
    f = _fetcher(tmp_path, archive)
    first = f.fetch(archive.list_location(5, date(2025, 1, 1), date(2025, 12, 31)))
    second = f.fetch(archive.list_location(5, date(2025, 1, 1), date(2025, 12, 31)))
    assert first == second == {KEY: sha256_hex(b"v1")}
    assert archive.downloads == [KEY]
    assert (tmp_path / "landing" / KEY).read_bytes() == b"v1"


def test_revision_keeps_both_versions(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(KEY, b"v1")
    f = _fetcher(tmp_path, archive)
    f.fetch([archive.info(KEY)])
    archive.put(KEY, b"v2-patched")
    f.fetch([archive.info(KEY)])
    store = RawStore(tmp_path / "raw")
    assert store.get(sha256_hex(b"v1")) == b"v1"
    assert store.get(sha256_hex(b"v2-patched")) == b"v2-patched"
    assert (tmp_path / "landing" / KEY).read_bytes() == b"v2-patched"  # landing = latest
    assert len(FetchIndex(tmp_path / "raw" / "index.csv").load()) == 2


def test_truncated_download_is_rejected(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(KEY, b"0123456789")
    archive.truncate_next = True
    f = _fetcher(tmp_path, archive)
    with pytest.raises(SizeMismatchError):
        f.fetch([archive.info(KEY)])
    assert FetchIndex(tmp_path / "raw" / "index.csv").load() == {}
    assert not (tmp_path / "landing" / KEY).exists()
    f.fetch([archive.info(KEY)])  # the retry stores it correctly
    assert (tmp_path / "landing" / KEY).read_bytes() == b"0123456789"
