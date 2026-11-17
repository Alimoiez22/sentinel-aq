"""Compare two quality-rule versions on the same raw files (reproducible evidence, rule 4).

The rules are a pure function of (file, location context, config), so re-evaluating every
current raw file under the old and the new config shows exactly what a version change did,
without depending on the database state at the time it happened.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from sentinel.contract import ContractError, parse_file
from sentinel.openaq.archive import parse_key
from sentinel.pipeline import landing_files
from sentinel.quality import DayContext, QualityConfig, evaluate

OUT = Path("results/quality_rules_v1_vs_v2.csv")
OUT_DAYS = Path("results/quality_rules_v1_vs_v2_by_rule.csv")


def compare(
    old: QualityConfig,
    new: QualityConfig,
    landing: Path,
    locations_csv: Path,
    location_ids: list[int],
    parameters: tuple[str, ...],
    now: datetime | None = None,
) -> tuple[Path, Path]:
    """Blocked days and per-rule counts under both configs, per location."""
    now = now or datetime.now(UTC)
    locs = {r["location_id"]: r for r in
            pl.read_csv(locations_csv, infer_schema_length=None).iter_rows(named=True)}  # fmt: skip
    per_loc: dict[tuple[int, str], dict[str, int]] = {}
    rules: dict[tuple[int, str, str], int] = {}
    for key, path in landing_files(landing, location_ids):
        loc, day = parse_key(key)
        meta = locs[loc]
        ctx = DayContext(
            loc,
            day,
            meta["timezone"],
            parameters,
            now,
            (float(meta["latitude"]), float(meta["longitude"])),
        )
        try:
            frame = parse_file(path.read_bytes()).frame
        except ContractError:
            continue
        for label, cfg in (("v1", old), ("v2", new)):
            r = evaluate(frame, cfg, ctx)
            s = per_loc.setdefault(
                (loc, label),
                {"days": 0, "blocked_days": 0, "accepted_rows": 0, "quarantined_rows": 0},
            )
            s["days"] += 1
            s["blocked_days"] += int(r.blocked)
            s["accepted_rows"] += r.accepted.height
            s["quarantined_rows"] += r.rejects.height
            for rid, n in r.counts.items():
                if rid.startswith("Q") and n and rid != "Q16_out_of_scope":
                    rules[(loc, label, rid)] = rules.get((loc, label, rid), 0) + n
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"location_id": k[0], "rules": k[1], **v} for k, v in sorted(per_loc.items())]
                 ).write_csv(OUT)  # fmt: skip
    pl.DataFrame([{"location_id": k[0], "rules": k[1], "rule_id": k[2], "rows": v}
                  for k, v in sorted(rules.items())]).write_csv(OUT_DAYS)  # fmt: skip
    return OUT, OUT_DAYS
