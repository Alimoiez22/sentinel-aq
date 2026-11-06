"""Generate SYNTHETIC OpenAQ-style daily files for tests (never real measurements).

The files copy the archive's layout and both header variants seen in the docs:

* variant A, the docs' column table: ``sensor_id``, ``unit``
* variant B, the docs' example and a real file read on 2026-11-05: ``sensors_id``, ``units``

Output is byte-identical on every run: a fixed seed, a fixed gzip mtime (0) and no
file name stored in the gzip header. ``tests/unit/test_fixtures.py`` checks that.

Usage: ``python scripts/make_fixtures.py [--out data/fixtures/synthetic]``
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

SEED = 20261106
PKT = timezone(timedelta(hours=5), "PKT")  # Pakistan Standard Time, UTC+05:00
DAYS = (date(2025, 11, 1), date(2025, 11, 2), date(2025, 11, 3))

HEADER_A = [
    "location_id",
    "sensor_id",
    "location",
    "datetime",
    "lat",
    "lon",
    "parameter",
    "unit",
    "value",
]
HEADER_B = [
    "location_id",
    "sensors_id",
    "location",
    "datetime",
    "lat",
    "lon",
    "parameter",
    "units",
    "value",
]


@dataclass(frozen=True)
class SyntheticLocation:
    """A made-up location. Ids start at 99 000 000 so they are easy to spot as fake."""

    location_id: int
    name: str
    lat: float
    lon: float
    header: list[str]
    sensors: tuple[tuple[int, str], ...]  # (sensor id, parameter)


LOCATIONS = (
    SyntheticLocation(
        99000001,
        "SYNTHETIC-A",
        31.5000,
        74.3000,
        HEADER_A,
        ((99100011, "pm25"), (99100012, "pm10")),
    ),
    SyntheticLocation(
        99000002,
        "SYNTHETIC-B",
        31.5500,
        74.3500,
        HEADER_B,
        ((99100021, "pm25"), (99100022, "pm10")),
    ),
)


def _rows(loc: SyntheticLocation, day: date, rng: random.Random) -> list[list[str]]:
    """24 hourly rows per sensor. Values are lognormal draws, not measurements."""
    rows: list[list[str]] = []
    for sensor_id, parameter in loc.sensors:
        median = 120.0 if parameter == "pm25" else 220.0  # arbitrary, smog-season-like scale
        # Hours 01:00..24:00. The real file read on 2026-11-05 started at 01:00, which
        # suggests stamps mark the END of the hour. One file is not proof: checked in M2.
        for hour in range(1, 25):
            ts = datetime(day.year, day.month, day.day, tzinfo=PKT) + timedelta(hours=hour)
            value = round(rng.lognormvariate(0.0, 0.35) * median, 1)
            rows.append(
                [
                    str(loc.location_id),
                    str(sensor_id),
                    f"{loc.name}-{loc.location_id}",
                    ts.isoformat(),
                    f"{loc.lat:.4f}",
                    f"{loc.lon:.4f}",
                    parameter,
                    "µg/m³",
                    str(value),
                ]
            )
    return rows


def _gzip_bytes(text: str) -> bytes:
    """Gzip with mtime=0 and an empty stored file name, so equal input gives equal bytes."""
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0) as gz:
        gz.write(text.encode("utf-8"))
    return buf.getvalue()


def archive_path(root: Path, location_id: int, day: date) -> Path:
    """Path in the OpenAQ archive layout (see docs/RESEARCH.md section 5)."""
    return (
        root
        / "records"
        / "csv.gz"
        / f"locationid={location_id}"
        / f"year={day.year:04d}"
        / f"month={day.month:02d}"
        / f"location-{location_id}-{day:%Y%m%d}.csv.gz"
    )


def generate(out: Path) -> dict[str, str]:
    """Write every fixture file under `out`. Returns {relative path: sha256}."""
    rng = random.Random(SEED)  # noqa: S311 - seeded test data, not security-related
    checksums: dict[str, str] = {}
    for loc in LOCATIONS:
        for day in DAYS:
            text = io.StringIO()
            writer = csv.writer(text, lineterminator="\n")
            writer.writerow(loc.header)
            writer.writerows(_rows(loc, day, rng))
            data = _gzip_bytes(text.getvalue())
            path = archive_path(out, loc.location_id, day)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            checksums[path.relative_to(out).as_posix()] = hashlib.sha256(data).hexdigest()
    manifest = "path,sha256\n" + "".join(f"{p},{h}\n" for p, h in sorted(checksums.items()))
    (out / "MANIFEST.csv").write_text(manifest, encoding="utf-8", newline="\n")
    return checksums


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=Path("data/fixtures/synthetic"))
    args = parser.parse_args()
    files = generate(args.out)
    print(f"wrote {len(files)} synthetic files to {args.out}")


if __name__ == "__main__":
    main()
