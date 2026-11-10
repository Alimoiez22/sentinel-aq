"""An in-memory archive for tests: objects can be added, revised and removed."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime

from sentinel.http import HttpClient
from sentinel.openaq.archive import Archive, ObjectInfo, month_prefix, parse_key


def day_key(location_id: int, d: date) -> str:
    """Archive key of one location-day."""
    return f"{month_prefix(location_id, d.year, d.month)}location-{location_id}-{d:%Y%m%d}.csv.gz"


class StubArchive(Archive):
    """Behaves like Archive, without a network. `downloads` counts GETs."""

    def __init__(self) -> None:
        super().__init__(HttpClient())
        self.objects: dict[str, tuple[bytes, datetime]] = {}
        self.downloads: list[str] = []
        self.truncate_next = False

    def put(self, key: str, data: bytes, modified: datetime | None = None) -> None:
        """Add or revise an object (a revision gets a new ETag automatically)."""
        self.objects[key] = (data, modified or datetime(2025, 11, 5, 7, 0, tzinfo=UTC))

    def info(self, key: str) -> ObjectInfo:
        data, modified = self.objects[key]
        return ObjectInfo(key, len(data), hashlib.md5(data).hexdigest(), modified)  # noqa: S324 - mimics S3 ETag

    def list_location(self, location_id: int, start: date, end: date) -> list[ObjectInfo]:
        out = []
        for key in self.objects:
            loc, d = parse_key(key)
            if loc == location_id and start <= d <= end:
                out.append(self.info(key))
        return sorted(out)

    def download(self, key: str) -> bytes:
        self.downloads.append(key)
        data = self.objects[key][0]
        if self.truncate_next:
            self.truncate_next = False
            return data[: len(data) // 2]
        return data
