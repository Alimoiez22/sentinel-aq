"""Snapshot diff (pure) and snapshot runs (with an in-memory archive)."""

from __future__ import annotations

import csv
import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from sentinel.fetch import Fetcher, FetchIndex
from sentinel.rawstore import RawStore, sha256_hex
from sentinel.snapshot import Entry, Snapshotter, diff
from tests.stubs import StubArchive, day_key

entries = st.builds(
    Entry,
    etag=st.sampled_from(["e1", "e2", "e3"]),
    size=st.integers(1, 3),
    last_modified=st.sampled_from(["t1", "t2"]),
    sha256=st.just(""),
)
listings = st.dictionaries(st.sampled_from([f"k{i}" for i in range(8)]), entries, max_size=8)


def test_diff_classifies_each_case() -> None:
    prev = {"same": Entry("a", 1, "t"), "rev": Entry("a", 1, "t"), "gone": Entry("a", 1, "t")}
    cur = {"same": Entry("a", 1, "t"), "rev": Entry("b", 1, "t2"), "new": Entry("c", 2, "t")}
    assert {r["key"]: r["change"] for r in diff(prev, cur)} == {
        "rev": "changed", "gone": "disappeared", "new": "new"
    }  # fmt: skip


@given(listings)
def test_no_change_means_no_diff(listing: dict[str, Entry]) -> None:
    assert diff(listing, dict(listing)) == []


@given(listings, listings, st.randoms())
def test_diff_does_not_depend_on_order(
    prev: dict[str, Entry], cur: dict[str, Entry], rnd: random.Random
) -> None:
    shuffled = list(cur.items())
    rnd.shuffle(shuffled)
    assert diff(prev, cur) == diff(prev, dict(shuffled))


@given(listings, listings)
def test_every_difference_is_reported_once(prev: dict[str, Entry], cur: dict[str, Entry]) -> None:
    keys = [r["key"] for r in diff(prev, cur)]
    assert len(keys) == len(set(keys))

    def version(e: Entry | None) -> tuple[str, int, str] | None:
        return None if e is None else (e.etag, e.size, e.last_modified)

    expected = {k for k in prev.keys() | cur.keys() if version(prev.get(k)) != version(cur.get(k))}
    assert set(keys) == expected


T0 = datetime(2026, 11, 11, 3, 0, tzinfo=UTC)
D1, D2 = date(2025, 11, 1), date(2025, 11, 2)


def _snap(tmp_path: Path, archive: StubArchive) -> Snapshotter:
    fetcher = Fetcher(
        archive,
        RawStore(tmp_path / "raw"),
        tmp_path / "landing",
        FetchIndex(tmp_path / "raw" / "index.csv"),
    )
    return Snapshotter(archive, fetcher, tmp_path / "snapshots")


def _changes(run_dir: Path) -> list[dict[str, str]]:
    with (run_dir / "changes.csv").open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_runs_record_new_unchanged_and_revised(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(day_key(5, D1), b"day1-v1")
    snap = _snap(tmp_path, archive)

    r1 = snap.run([5], D1, D2, now=T0)
    assert [c["change"] for c in _changes(r1)] == ["new"]

    r2 = snap.run([5], D1, D2, now=T0 + timedelta(days=1))
    assert _changes(r2) == []  # nothing changed upstream
    assert archive.downloads == [day_key(5, D1)]  # and nothing was downloaded again

    archive.put(day_key(5, D1), b"day1-v2", modified=T0)
    archive.put(day_key(5, D2), b"day2-v1")
    r3 = snap.run([5], D1, D2, now=T0 + timedelta(days=2))
    changes = {c["key"]: c for c in _changes(r3)}
    rev = changes[day_key(5, D1)]
    assert rev["change"] == "changed"
    assert (rev["old_sha256"], rev["new_sha256"]) == (
        sha256_hex(b"day1-v1"),
        sha256_hex(b"day1-v2"),
    )
    assert changes[day_key(5, D2)]["change"] == "new"
    meta = json.loads((r3 / "meta.json").read_text(encoding="utf-8"))
    assert meta["previous_run"] == r2.name
    assert meta["n_changes"] == {"new": 1, "changed": 1, "disappeared": 0}
    store = RawStore(tmp_path / "raw")
    assert store.get(sha256_hex(b"day1-v1")) == b"day1-v1"  # old version still kept


def test_disappeared_file_is_reported(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(day_key(5, D1), b"x")
    snap = _snap(tmp_path, archive)
    snap.run([5], D1, D2, now=T0)
    del archive.objects[day_key(5, D1)]
    r2 = snap.run([5], D1, D2, now=T0 + timedelta(days=1))
    assert [c["change"] for c in _changes(r2)] == ["disappeared"]


def test_failed_run_leaves_no_snapshot(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(day_key(5, D1), b"0123456789")
    archive.truncate_next = True
    snap = _snap(tmp_path, archive)
    with pytest.raises(RuntimeError):
        snap.run([5], D1, D2, now=T0)
    assert not (tmp_path / "snapshots").exists() or not any((tmp_path / "snapshots").iterdir())


def test_leftover_temp_dir_is_not_used_as_previous(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(day_key(5, D1), b"x")
    junk = tmp_path / "snapshots" / ".tmp-19990101T000000Z"
    junk.mkdir(parents=True)
    (junk / "listing.csv").write_text("key,size_bytes,etag,last_modified_utc,sha256\n")
    r = _snap(tmp_path, archive).run([5], D1, D2, now=T0)
    assert json.loads((r / "meta.json").read_text(encoding="utf-8"))["previous_run"] is None


def test_never_overwrites_a_run(tmp_path: Path) -> None:
    archive = StubArchive()
    archive.put(day_key(5, D1), b"x")
    snap = _snap(tmp_path, archive)
    snap.run([5], D1, D2, now=T0)
    with pytest.raises(FileExistsError):
        snap.run([5], D1, D2, now=T0)
