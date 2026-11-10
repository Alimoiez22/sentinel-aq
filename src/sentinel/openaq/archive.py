"""Anonymous read access to the OpenAQ archive bucket over HTTPS.

Layout (docs/RESEARCH.md §5): records/csv.gz/locationid={id}/year={YYYY}/month={MM}/
location-{id}-{YYYYMMDD}.csv.gz. We use two S3 REST calls only: ListObjectsV2 (paged with
a continuation token) and GET object. No AWS SDK is needed for anonymous reads (ADR-010).
"""

from __future__ import annotations

import re
import urllib.parse
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime

from sentinel.http import HttpClient

BUCKET_URL = "https://openaq-data-archive.s3.amazonaws.com"
_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
_KEY_RE = re.compile(
    r"^records/csv\.gz/locationid=(?P<loc>\d+)/year=(?P<y>\d{4})/month=(?P<m>\d{2})/"
    r"location-(?P=loc)-(?P<d>\d{8})\.csv\.gz$"
)


@dataclass(frozen=True, order=True)
class ObjectInfo:
    """One object in a listing. `etag` is S3's opaque version tag, quotes removed."""

    key: str
    size: int
    etag: str
    last_modified: datetime


def month_prefix(location_id: int, year: int, month: int) -> str:
    """Listing prefix for one location-month."""
    return f"records/csv.gz/locationid={location_id}/year={year:04d}/month={month:02d}/"


def parse_key(key: str) -> tuple[int, date]:
    """Return (location_id, source_date) of an archive key; ValueError if not a day file."""
    m = _KEY_RE.match(key)
    if m is None:
        raise ValueError(f"not an archive day-file key: {key}")
    d = datetime.strptime(m["d"], "%Y%m%d").date()
    if (d.year, d.month) != (int(m["y"]), int(m["m"])):
        raise ValueError(f"date in file name does not match its partition: {key}")
    return int(m["loc"]), d


def parse_listing(xml_bytes: bytes) -> tuple[list[ObjectInfo], str | None]:
    """Parse one ListObjectsV2 page into objects and the next continuation token."""
    root = ET.fromstring(xml_bytes)  # noqa: S314 - S3 response
    objects = []
    for c in root.findall("s3:Contents", _NS):
        objects.append(
            ObjectInfo(
                key=_text(c, "Key"),
                size=int(_text(c, "Size")),
                etag=_text(c, "ETag").strip('"'),
                last_modified=datetime.fromisoformat(
                    _text(c, "LastModified").replace("Z", "+00:00")
                ),
            )
        )
    truncated = root.findtext("s3:IsTruncated", default="false", namespaces=_NS) == "true"
    token = root.findtext("s3:NextContinuationToken", namespaces=_NS) if truncated else None
    return objects, token


def _text(elem: ET.Element, tag: str) -> str:
    value = elem.findtext(f"s3:{tag}", namespaces=_NS)
    if value is None:
        raise ValueError(f"S3 listing entry without <{tag}>")
    return value


@dataclass
class Archive:
    """Lists and downloads archive objects. `base_url` is swapped for a fake in tests."""

    http: HttpClient
    base_url: str = BUCKET_URL

    def list_prefix(self, prefix: str) -> list[ObjectInfo]:
        """All objects under `prefix`, following continuation tokens."""
        out: list[ObjectInfo] = []
        token: str | None = None
        while True:
            params = {"list-type": "2", "prefix": prefix}
            if token:
                params["continuation-token"] = token
            url = f"{self.base_url}/?{urllib.parse.urlencode(params)}"
            page, token = parse_listing(self.http.get(url).body)
            out.extend(page)
            if token is None:
                return sorted(out)

    def list_location(self, location_id: int, start: date, end: date) -> list[ObjectInfo]:
        """Day files of one location with source dates in [start, end]."""
        out: list[ObjectInfo] = []
        for year, month in months(start, end):
            for obj in self.list_prefix(month_prefix(location_id, year, month)):
                try:
                    _, d = parse_key(obj.key)
                except ValueError:
                    continue  # not a day file; never guessed (reported by coverage)
                if start <= d <= end:
                    out.append(obj)
        return out

    def download(self, key: str) -> bytes:
        """Raw bytes of one object (still gzipped)."""
        return self.http.get(f"{self.base_url}/{urllib.parse.quote(key)}").body


def months(start: date, end: date) -> Iterator[tuple[int, int]]:
    """(year, month) pairs covering [start, end]."""
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
