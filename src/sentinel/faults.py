"""Fault catalogue F1-F15 (docs/FAULTS.md): seeded, deterministic trial definitions.

A trial starts from a clean dataset (one archive key -> bytes per location-day) and
describes:
* source_at[r]  - what the upstream archive holds at scheduled run r (refetch/backfill
                  read from here; for faults in the data itself the source is faulty too)
* deliveries[r] - files dropped into the landing folder just before scheduled run r
* crash         - an armed crash point (F10-F12)
* truth         - the input golden is built from: the clean file for transfer and
                  pipeline faults; the faulty file itself for faults in the upstream data
                  (a correct pipeline must quarantine/block it, not repair it)
* expected      - the handling FAULTS.md defines as correct (used to judge
                  "inappropriate automatic action")
"""

from __future__ import annotations

import csv
import gzip
import io
import random
from dataclasses import dataclass, field
from datetime import date

from sentinel.openaq.archive import parse_key

FAULTS = ["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10", "F11", "F12",
          "F13", "F14", "F15"]  # fmt: skip
N_RUNS = 2  # scheduled daily runs per trial, in every mode


@dataclass(frozen=True)
class Expected:
    """Correct handling of a fault: its class, and whether automation may fix it."""

    failure_class: str | None  # None: the pipeline handles it without an incident
    automatic: bool  # True: an allow-listed action should recover it
    escalate: bool  # True: a human must decide (correct outcome = escalation)


EXPECTED: dict[str, Expected] = {
    "F0": Expected(None, True, False),
    "F1": Expected(None, True, False),  # duplicate: content-addressed no-op
    "F2": Expected("transfer_corruption", True, False),
    "F3": Expected("rows_over_threshold", True, True),  # block automatically, then escalate
    "F4": Expected(None, True, False),  # known variant: mapped by the contract
    "F5": Expected("unknown_schema", False, True),
    "F6": Expected("rows_over_threshold", True, True),
    "F7": Expected("missing_file", True, False),
    "F8": Expected("late_file", True, False),
    "F9": Expected("revision", True, False),
    "F10": Expected("process_crash", True, False),
    "F11": Expected("process_crash", True, False),
    "F12": Expected("transient_db", True, False),
    "F13": Expected("unknown_unit", False, True),
    "F14": Expected(None, True, False),  # flagged and kept
    "F15": Expected("timezone_offset", False, True),
}


@dataclass
class Trial:
    """Everything one fault-injection trial needs."""

    fault: str
    seed: int
    target: str
    source_at: list[dict[str, bytes]]
    deliveries: list[dict[str, bytes]]
    truth: dict[tuple[int, date], bytes]
    crash: tuple[int, str, str] | None = None  # (run index, crash point, how)
    notes: dict[str, object] = field(default_factory=dict)

    @property
    def expected(self) -> Expected:
        return EXPECTED[self.fault]

    @property
    def expected_days(self) -> set[tuple[int, date]]:
        return set(self.truth)


# ------------------------------------------------------------- file mutations
def _read(data: bytes) -> tuple[list[str], list[list[str]]]:
    rows = list(csv.reader(io.StringIO(gzip.decompress(data).decode("utf-8"))))
    return rows[0], rows[1:]


def _write(header: list[str], rows: list[list[str]]) -> bytes:
    out = io.StringIO()
    w = csv.writer(out, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    return gzip.compress(out.getvalue().encode("utf-8"), mtime=0)


def truncate(data: bytes, rng: random.Random) -> bytes:
    """F2: a transfer cut short (between 30 % and 70 % of the bytes)."""
    return data[: int(len(data) * rng.uniform(0.3, 0.7))]


def corrupt_rows(data: bytes, rng: random.Random, share: float) -> bytes:
    """F3: `share` of the values replaced by garbage upstream."""
    h, rows = _read(data)
    vi = h.index("value")
    for i in rng.sample(range(len(rows)), k=max(1, int(len(rows) * share))):
        rows[i][vi] = "#ERR"
    return _write(h, rows)


def swap_variant(data: bytes) -> bytes:
    """F4: the other KNOWN header variant (sensor_id/unit <-> sensors_id/units)."""
    h, rows = _read(data)
    swap = {"sensor_id": "sensors_id", "sensors_id": "sensor_id", "unit": "units", "units": "unit"}
    return _write([swap.get(c, c) for c in h], rows)


def add_column(data: bytes) -> bytes:
    """F5: an unannounced extra column upstream (unknown schema)."""
    h, rows = _read(data)
    return _write([*h, "qc_flag"], [[*r, "0"] for r in rows])


def comma_decimals(data: bytes) -> bytes:
    """F6: type drift, decimal comma ("12,3") upstream."""
    h, rows = _read(data)
    vi = h.index("value")
    for r in rows:
        r[vi] = r[vi].replace(".", ",")
    return _write(h, rows)


def _series(h: list[str], rows: list[list[str]], parameter: str = "pm25") -> list[int]:
    """Row indexes of one parameter's series in time order (the series the pipeline
    publishes). Falls back to every row when the file has no such parameter.

    Injector v2 (2026-11-17): v1 mutated rows in FILE order, which on real multi-parameter
    files (AirGradient: pm1, pm25, rh, temperature, um003) hit only out-of-scope
    parameters, so F13/F14 never manifested in the published series. See ADR-022.
    """
    pi, di = h.index("parameter"), h.index("datetime")
    idx = [i for i, r in enumerate(rows) if r[pi] == parameter]
    return sorted(idx, key=lambda i: rows[i][di]) if idx else list(range(len(rows)))


def unit_change(data: bytes, rng: random.Random) -> bytes:
    """F13: the PM2.5 series switches to ppm part-way through the day (not convertible)."""
    h, rows = _read(data)
    ui = h.index("unit") if "unit" in h else h.index("units")
    series = _series(h, rows)
    start = rng.randint(len(series) // 3, len(series) // 2)
    for i in series[start:]:
        rows[i][ui] = "ppm"
    return _write(h, rows)


def stuck_and_spikes(data: bytes, rng: random.Random) -> bytes:
    """F14: an 8-hour stuck run plus a burst of 3 large spikes, in the PM2.5 series."""
    h, rows = _read(data)
    vi = h.index("value")
    series = _series(h, rows)
    start = rng.randint(0, max(0, min(10, len(series) - 8)))
    for i in series[start : start + 8]:
        rows[i][vi] = rows[series[start]][vi]
    rest = [i for i in series if i not in series[start : start + 8]]
    for i in rng.sample(rest, k=min(3, len(rest))):
        rows[i][vi] = f"{float(rows[i][vi]) * 6:.1f}"
    return _write(h, rows)


def wrong_offset(data: bytes) -> bytes:
    """F15: local wall-clock times written with a +00:00 offset upstream."""
    h, rows = _read(data)
    di = h.index("datetime")
    for r in rows:
        r[di] = r[di][:-6] + "+00:00"
    return _write(h, rows)


def revise(data: bytes, rng: random.Random) -> bytes:
    """F9: a retroactive patch changing 3-6 values by 10-50 %."""
    h, rows = _read(data)
    vi = h.index("value")
    for i in rng.sample(range(len(rows)), k=rng.randint(3, 6)):
        rows[i][vi] = f"{float(rows[i][vi]) * rng.uniform(1.1, 1.5):.1f}"
    return _write(h, rows)


# ------------------------------------------------------------------ trials
def build_trial(fault: str, seed: int, clean: dict[str, bytes]) -> Trial:
    """Deterministic trial for (fault, seed) over a clean dataset {key: bytes}."""
    if fault not in EXPECTED:
        raise ValueError(f"unknown fault {fault}")
    rng = random.Random(f"{fault}-{seed}")  # noqa: S311 - seeded fault parameters, not security
    keys = sorted(clean)
    last_day = max(parse_key(k)[1] for k in keys)
    # F8 (late) must hit the newest day: older days are past their grace window.
    candidates = [k for k in keys if parse_key(k)[1] == last_day] if fault == "F8" else keys
    target = rng.choice(candidates)
    src = dict(clean)
    first = dict(clean)
    second: dict[str, bytes] = {}
    truth = dict(clean)
    crash = None

    def upstream(mutated: bytes) -> None:  # a fault in the data itself: source is faulty
        src[target] = first[target] = truth[target] = mutated

    if fault == "F1":
        second[target] = clean[target]  # identical bytes delivered again
    elif fault == "F2":
        first[target] = truncate(clean[target], rng)
    elif fault == "F3":
        upstream(corrupt_rows(clean[target], rng, rng.uniform(0.3, 0.45)))
    elif fault == "F4":
        upstream(swap_variant(clean[target]))
    elif fault == "F5":
        src[target] = first[target] = add_column(clean[target])  # truth stays the content
    elif fault == "F6":
        upstream(comma_decimals(clean[target]))
    elif fault == "F7":
        del first[target]  # the fetch missed it; the source has it
    elif fault == "F8":
        del first[target]
        second[target] = clean[target]
    elif fault == "F9":
        second[target] = truth[target] = revise(clean[target], rng)
    elif fault in ("F10", "F11", "F12"):
        point = {"F10": "after_staging_before_commit", "F11": "mid_mart_refresh",
                 "F12": "after_staging_before_commit"}[fault]  # fmt: skip
        crash = (0, point, "drop_connection" if fault == "F12" else "raise")
    elif fault == "F13":
        upstream(unit_change(clean[target], rng))
    elif fault == "F14":
        upstream(stuck_and_spikes(clean[target], rng))
    elif fault == "F15":
        upstream(wrong_offset(clean[target]))

    source_at = [dict(src) for _ in range(N_RUNS)]
    if fault == "F8":
        del source_at[0][target]  # not published upstream yet at run 0
    if fault == "F9":
        source_at[1][target] = second[target]
    return Trial(
        fault,
        seed,
        target,
        source_at,
        [first, second],
        {parse_key(k): v for k, v in truth.items()},
        crash,
    )
