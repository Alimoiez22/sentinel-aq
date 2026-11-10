"""Write-once content-addressed store."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from sentinel import rawstore
from sentinel.rawstore import CorruptObjectError, RawStore, sha256_hex


def test_put_is_idempotent(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    d1, created1 = store.put(b"hello")
    d2, created2 = store.put(b"hello")
    assert d1 == d2 == sha256_hex(b"hello")
    assert (created1, created2) == (True, False)
    assert store.path_for(d1).parent.name == d1[:2]


def test_two_versions_are_both_kept(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    a, _ = store.put(b"version 1")
    b, _ = store.put(b"version 2")
    assert a != b
    assert store.get(a) == b"version 1" and store.get(b) == b"version 2"


def test_corruption_is_detected(tmp_path: Path) -> None:
    store = RawStore(tmp_path)
    digest, _ = store.put(b"original")
    path = store.path_for(digest)
    path.chmod(0o644)
    path.write_bytes(b"tampered")
    with pytest.raises(CorruptObjectError):
        store.get(digest)
    with pytest.raises(CorruptObjectError):
        store.put(b"original")  # re-put verifies the existing copy, does not trust it


def test_failed_write_leaves_nothing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = RawStore(tmp_path)

    def boom(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)  # rawstore calls os.replace
    with pytest.raises(OSError, match="disk full"):
        store.put(b"data")
    assert not store.has(sha256_hex(b"data"))
    assert [p for p in tmp_path.rglob("*") if p.is_file()] == []  # no temp file left


@pytest.mark.parametrize("bad", ["", "abc", "Z" * 64, "../" + "a" * 61])
def test_rejects_non_digests(tmp_path: Path, bad: str) -> None:
    with pytest.raises(ValueError):
        RawStore(tmp_path).path_for(bad)


@given(st.binary(max_size=2048))
def test_round_trip(data: bytes) -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        store = RawStore(Path(d))
        digest, _ = store.put(data)
        assert store.get(digest) == data


def test_landing_write_is_atomic_replace(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b.csv.gz"
    rawstore.write_atomic(target, b"v1")
    rawstore.write_atomic(target, b"v2")
    assert target.read_bytes() == b"v2"
    assert sorted(os.listdir(target.parent)) == ["b.csv.gz"]
