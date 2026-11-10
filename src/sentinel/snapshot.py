"""Daily snapshot of the archive for Track B (real revisions).

The bucket keeps no old versions (RESEARCH §5.2), so the only evidence that a file was
patched is our own record. Each run writes, under data/snapshots/<UTC time>/:

* listing.csv: every key for the snapshot locations, with size, ETag, LastModified and
  the SHA-256 of the stored bytes;
* changes.csv: the difference to the previous run (new / changed / disappeared);
* meta.json: when, what scope, counts.

Changed or new versions are downloaded into the write-once raw store, so the old bytes
of a revised file stay available for the M6 old-vs-new diff.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path

from sentinel.fetch import Fetcher
from sentinel.openaq.archive import Archive, ObjectInfo

LISTING_COLUMNS = ["key", "size_bytes", "etag", "last_modified_utc", "sha256"]
CHANGE_COLUMNS = ["key", "change", "old_etag", "new_etag", "old_sha256", "new_sha256",
                  "old_last_modified_utc", "new_last_modified_utc"]  # fmt: skip


class Change(StrEnum):
    """How a key differs from the previous snapshot."""

    NEW = "new"
    CHANGED = "changed"
    DISAPPEARED = "disappeared"


@dataclass(frozen=True)
class Entry:
    """One key as recorded in a snapshot listing."""

    etag: str
    size: int
    last_modified: str
    sha256: str = ""


def diff(prev: Mapping[str, Entry], cur: Mapping[str, Entry]) -> list[dict[str, str]]:
    """Changes from `prev` to `cur`, sorted by key. Unchanged keys are not listed.

    A key counts as changed if its ETag, size or LastModified differs. Comparing three
    fields means a change S3 reports in any of them is caught; the SHA-256 of the
    downloaded bytes later confirms whether the content really differs.
    """
    out = []
    for key in sorted(prev.keys() | cur.keys()):
        p, c = prev.get(key), cur.get(key)
        if p is None and c is not None:
            kind = Change.NEW
        elif c is None and p is not None:
            kind = Change.DISAPPEARED
        elif (
            p is not None
            and c is not None
            and (p.etag, p.size, p.last_modified) != (c.etag, c.size, c.last_modified)
        ):
            kind = Change.CHANGED
        else:
            continue
        out.append({
            "key": key, "change": kind.value,
            "old_etag": p.etag if p else "", "new_etag": c.etag if c else "",
            "old_sha256": p.sha256 if p else "", "new_sha256": c.sha256 if c else "",
            "old_last_modified_utc": p.last_modified if p else "",
            "new_last_modified_utc": c.last_modified if c else "",
        })  # fmt: skip
    return out


def read_listing(path: Path) -> dict[str, Entry]:
    """Load a listing.csv written by a previous run."""
    with path.open(encoding="utf-8", newline="") as f:
        return {
            r["key"]: Entry(r["etag"], int(r["size_bytes"]), r["last_modified_utc"], r["sha256"])
            for r in csv.DictReader(f)
        }


def previous_listing(root: Path, current_run: str) -> tuple[str | None, dict[str, Entry]]:
    """The latest earlier run (run ids sort chronologically) and its listing."""
    runs = sorted(
        p.parent.name
        for p in root.glob("*/listing.csv")
        if not p.parent.name.startswith(".") and p.parent.name < current_run
    )  # pathlib's "*" also matches dot-dirs, so leftover .tmp-* dirs are excluded here
    if not runs:
        return None, {}
    return runs[-1], read_listing(root / runs[-1] / "listing.csv")


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


@dataclass
class Snapshotter:
    """Runs one snapshot: list, fetch new/changed versions, record, diff."""

    archive: Archive
    fetcher: Fetcher
    root: Path

    def run(
        self, location_ids: list[int], start: date, end: date, now: datetime | None = None
    ) -> Path:
        """Take a snapshot and return its directory."""
        now = now or datetime.now(UTC)
        run_id = now.strftime("%Y%m%dT%H%M%SZ")
        out_dir = self.root / run_id
        if out_dir.exists():
            raise FileExistsError(
                f"snapshot {run_id} already exists; snapshots are never overwritten"
            )
        objects: list[ObjectInfo] = []
        for loc in sorted(set(location_ids)):
            objects.extend(self.archive.list_location(loc, start, end))
        shas = self.fetcher.fetch(objects)
        cur = {
            o.key: Entry(o.etag, o.size, o.last_modified.isoformat(), shas[o.key]) for o in objects
        }
        prev_run, prev = previous_listing(self.root, run_id)
        changes = diff(prev, cur)
        # Written to a hidden temp dir, then renamed: a crash never leaves a partial
        # snapshot that the next run would diff against (previous_listing skips .tmp-*).
        final_dir, out_dir = out_dir, self.root / f".tmp-{run_id}"
        out_dir.mkdir(parents=True)
        _write_csv(out_dir / "listing.csv", LISTING_COLUMNS, [
            {"key": k, "size_bytes": str(e.size), "etag": e.etag,
             "last_modified_utc": e.last_modified, "sha256": e.sha256}
            for k, e in sorted(cur.items())
        ])  # fmt: skip
        _write_csv(out_dir / "changes.csv", CHANGE_COLUMNS, changes)
        meta = {
            "run_id": run_id, "previous_run": prev_run, "location_ids": sorted(set(location_ids)),
            "date_start": start.isoformat(), "date_end": end.isoformat(), "n_keys": len(cur),
            "n_changes": {c.value: sum(r["change"] == c.value for r in changes) for c in Change},
        }  # fmt: skip
        (out_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
        out_dir.rename(final_dir)
        return final_dir
