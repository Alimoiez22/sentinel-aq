"""Download archive objects into the raw store and the landing mirror.

The fetch index (data/raw/index.csv) records every (key, etag) we have stored and its
SHA-256. It is append-only: a revision adds a row, it never edits one. Fetching the
same version twice downloads nothing (idempotent).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sentinel.log import get_logger
from sentinel.openaq.archive import Archive, ObjectInfo
from sentinel.rawstore import RawStore, write_atomic

INDEX_COLUMNS = ["key", "etag", "size_bytes", "last_modified_utc", "sha256", "fetched_utc"]


class SizeMismatchError(RuntimeError):
    """Downloaded byte count differs from the listing: truncated or changed mid-read."""


@dataclass
class FetchIndex:
    """Append-only CSV of stored object versions."""

    path: Path

    def load(self) -> dict[tuple[str, str], str]:
        """{(key, etag): sha256} for every version stored so far."""
        if not self.path.exists():
            return {}
        with self.path.open(encoding="utf-8", newline="") as f:
            return {(r["key"], r["etag"]): r["sha256"] for r in csv.DictReader(f)}

    def append(self, rows: list[dict[str, str]]) -> None:
        """Add rows; writes the header if the file is new."""
        if not rows:
            return
        new = not self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=INDEX_COLUMNS, lineterminator="\n")
            if new:
                w.writeheader()
            w.writerows(rows)


@dataclass
class Fetcher:
    """Downloads only versions not yet in the index."""

    archive: Archive
    store: RawStore
    landing_root: Path
    index: FetchIndex

    def fetch(self, objects: list[ObjectInfo]) -> dict[str, str]:
        """Ensure every object version is stored. Returns {key: sha256}."""
        log = get_logger(step="fetch")
        known = self.index.load()
        result: dict[str, str] = {}
        new_rows: list[dict[str, str]] = []
        for obj in sorted(objects):
            sha = known.get((obj.key, obj.etag))
            if sha is not None and self.store.has(sha):
                result[obj.key] = sha
                continue
            data = self.archive.download(obj.key)
            if len(data) != obj.size:
                raise SizeMismatchError(f"{obj.key}: listed {obj.size} B, got {len(data)} B")
            sha, created = self.store.put(data)
            write_atomic(self.landing_root / obj.key, data)
            new_rows.append({
                "key": obj.key, "etag": obj.etag, "size_bytes": str(obj.size),
                "last_modified_utc": obj.last_modified.isoformat(), "sha256": sha,
                "fetched_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            })  # fmt: skip
            result[obj.key] = sha
            log.info("stored", key=obj.key, sha256=sha, new_blob=created, bytes=len(data))
            self.index.append(new_rows[-1:])  # durable after each file: resumable
        return result
