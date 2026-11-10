"""S3 listing parsing, continuation tokens, key parsing and date filtering."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime
from urllib.parse import parse_qs, urlparse

import pytest

from sentinel.http import HttpClient
from sentinel.openaq.archive import Archive, month_prefix, months, parse_key, parse_listing
from tests.conftest import FakeServer, Handler

# Shape copied from a real response for location 2178, January 2024 (read 2026-11-05);
# the key, size and ETag of the first entry are the real ones.
REAL_PAGE = b"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>openaq-data-archive</Name>
<Prefix>records/csv.gz/locationid=2178/year=2024/month=01/</Prefix>
<NextContinuationToken>TOKEN-1</NextContinuationToken><KeyCount>1</KeyCount><MaxKeys>1</MaxKeys>
<IsTruncated>true</IsTruncated>
<Contents><Key>records/csv.gz/locationid=2178/year=2024/month=01/location-2178-20240101.csv.gz</Key>
<LastModified>2024-01-05T07:11:52.000Z</LastModified><ETag>&quot;4cb48f0cf2d7d510d1e17653c75ba4da&quot;</ETag>
<Size>1087</Size><StorageClass>STANDARD</StorageClass></Contents></ListBucketResult>"""


def _page(keys: list[str], token: str | None) -> bytes:
    contents = "".join(
        f"<Contents><Key>{k}</Key><LastModified>2024-01-05T07:00:00.000Z</LastModified>"
        f"<ETag>&quot;e{i}&quot;</ETag><Size>{100 + i}</Size></Contents>"
        for i, k in enumerate(keys)
    )
    trunc = (
        f"<IsTruncated>true</IsTruncated><NextContinuationToken>{token}</NextContinuationToken>"
        if token
        else "<IsTruncated>false</IsTruncated>"
    )
    return (
        '<?xml version="1.0"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"{trunc}{contents}</ListBucketResult>"
    ).encode()


def _key(loc: int, d: date) -> str:
    return f"{month_prefix(loc, d.year, d.month)}location-{loc}-{d:%Y%m%d}.csv.gz"


def test_parse_real_listing_page() -> None:
    objs, token = parse_listing(REAL_PAGE)
    assert token == "TOKEN-1"  # noqa: S105 - S3 continuation token, not a password
    assert len(objs) == 1
    o = objs[0]
    assert o.size == 1087
    assert o.etag == "4cb48f0cf2d7d510d1e17653c75ba4da"  # quotes stripped
    assert o.last_modified == datetime(2024, 1, 5, 7, 11, 52, tzinfo=UTC)


def test_parse_key() -> None:
    assert parse_key(_key(2178, date(2024, 1, 31))) == (2178, date(2024, 1, 31))


@pytest.mark.parametrize(
    "bad",
    [
        "records/csv.gz/locationid=2178/year=2024/month=01/location-9999-20240101.csv.gz",
        "records/csv.gz/locationid=2178/year=2024/month=02/location-2178-20240101.csv.gz",
        "records/csv.gz/locationid=2178/year=2024/month=01/readme.txt",
    ],
)
def test_parse_key_rejects_inconsistent_keys(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_key(bad)


def test_months_span_year_boundary() -> None:
    assert list(months(date(2024, 11, 15), date(2025, 2, 1))) == [
        (2024, 11), (2024, 12), (2025, 1), (2025, 2)
    ]  # fmt: skip


def test_list_follows_continuation_and_filters(
    fake_server: Callable[[Handler], FakeServer],
) -> None:
    loc = 42
    jan = [_key(loc, date(2024, 1, d)) for d in (1, 2, 3)]

    def handler(path: str, headers: dict[str, str]) -> tuple[int, dict[str, str], bytes]:
        q = parse_qs(urlparse(path).query)
        assert q["list-type"] == ["2"]
        if "continuation-token" not in q:
            return 200, {}, _page(jan[:2], "T2")
        assert q["continuation-token"] == ["T2"]
        return 200, {}, _page([jan[2], month_prefix(loc, 2024, 1) + "notes.txt"], None)

    srv = fake_server(handler)
    archive = Archive(HttpClient(sleep=lambda s: None), base_url=srv.url)
    objs = archive.list_location(loc, date(2024, 1, 2), date(2024, 1, 31))
    assert [o.key for o in objs] == jan[1:]  # Jan 1 outside range; notes.txt skipped
    assert len(srv.requests) == 2


def test_download_quotes_key(fake_server: Callable[[Handler], FakeServer]) -> None:
    srv = fake_server(lambda p, h: (200, {}, p.encode()))
    archive = Archive(HttpClient(), base_url=srv.url)
    key = _key(7, date(2024, 3, 9))
    assert archive.download(key) == ("/" + key.replace("=", "%3D")).encode()
