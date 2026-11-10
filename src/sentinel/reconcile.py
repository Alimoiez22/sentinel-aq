"""Golden rebuild and comparators (M7).

Golden = everything rebuilt from scratch, into the `golden` schema, from one input per
location-day (production: the latest non-rejected revision in the manifest; experiments:
the clean files the fault injector started from). It uses the SAME parse, rules, gate and
mart SQL as the live pipeline, so any difference is caused by HOW the live state was
reached (order, retries, crashes, faults), not by different logic.

Comparators report, per table:
* missing  - key in golden, not live
* extra    - key in live, not golden (the PK makes true duplicates impossible; "duplicate"
             rows in the brief's sense show up here as extra rows)
* wrong    - same key, different values
and an order-independent checksum per month (sum of row hashes), so two tables can be
compared without sorting.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from typing import Any

from sentinel.contract import ContractError, parse_file
from sentinel.pipeline import Conn, Pipeline, gate, refresh_marts, write_fact
from sentinel.quality import evaluate

GOLDEN_TABLES = ["golden.fact_measurement_hourly", "golden.mart_daily_location",
                 "golden.mart_exceedance_summary", "golden.publication_status"]  # fmt: skip


def manifest_inputs(conn: Conn, pipe: Pipeline) -> dict[tuple[int, date], bytes]:
    """Production golden input: latest non-rejected revision of every location-day."""
    rows = conn.execute(
        "SELECT DISTINCT ON (location_id, source_date) location_id, source_date, sha256"
        " FROM ops.batch_manifest WHERE status <> 'rejected'"
        " ORDER BY location_id, source_date, revision_no DESC"
    ).fetchall()
    return {(int(loc), d): pipe.store.get(sha) for loc, d, sha in rows}


def rebuild_golden(conn: Conn, pipe: Pipeline, inputs: Mapping[tuple[int, date], bytes]) -> int:
    """Rebuild the golden schema from `inputs` in one transaction. Returns location-days."""
    with conn.transaction():
        conn.execute("TRUNCATE " + ", ".join(GOLDEN_TABLES))
        for (loc, d), data in sorted(inputs.items()):
            try:
                frame = parse_file(data).frame
            except ContractError:
                continue  # an unreadable input publishes nothing, live or golden
            result = evaluate(frame, pipe.cfg, pipe.context(conn, loc, d))
            b = {
                "batch_id": 0,
                "location_id": loc,
                "source_date": d,
                "revision_no": 0,
                "blocked": result.blocked,
            }
            write_fact(conn, "golden", b, result)
            gate(conn, "golden", b, pipe.parameters, pipe.cfg.completeness_min_hours)
        for loc, d in sorted(inputs):
            refresh_marts(conn, "golden", loc, d, pipe.cfg.completeness_min_hours)
    return len(inputs)


@dataclass
class TableDiff:
    """Differences between the live and golden copies of one table."""

    table: str
    live_rows: int
    golden_rows: int
    missing: int
    extra: int
    wrong: int
    checksum_equal: bool
    examples: list[dict[str, Any]] = field(default_factory=list)

    @property
    def equal(self) -> bool:
        return self.missing == self.extra == self.wrong == 0


# (live table, golden table, key columns, compared columns, month expression)
COMPARISONS = [
    ("wh.fact_measurement_hourly", "golden.fact_measurement_hourly", ["sensor_id", "ts_utc"],
     ["location_id", "source_date", "parameter", "value", "value_orig", "unit_orig", "flags"],
     "date_trunc('month', ts_utc)"),
    ("wh.mart_daily_location", "golden.mart_daily_location",
     ["location_id", "local_date", "parameter"],
     ["daily_mean", "hours_observed", "completeness_pct", "exceeds_guideline",
      "guideline_value", "status_norm"], "date_trunc('month', local_date)"),
    ("wh.mart_exceedance_summary", "golden.mart_exceedance_summary",
     ["location_id", "month", "parameter"], ["complete_days", "exceedance_days"], "month"),
]  # fmt: skip


def _source(table: str) -> str:
    # 'revised' means "complete, and replaced an earlier published version": the golden
    # rebuild cannot know publication history, so both count as complete for comparison.
    if table.endswith("mart_daily_location"):
        return (
            f"(SELECT *, CASE WHEN publication_status = 'revised' THEN 'complete'"  # noqa: S608
            f" ELSE publication_status END AS status_norm FROM {table})"
        )
    return table


def compare_table(
    conn: Conn,
    live: str,
    golden: str,
    keys: list[str],
    cols: list[str],
    month_expr: str,
    location_ids: list[int] | None = None,
) -> TableDiff:
    """Key-based comparison plus order-independent checksums per month."""
    where = "WHERE location_id = ANY(%(locs)s)" if location_ids else ""
    params = {"locs": location_ids}
    k = ", ".join(keys)
    on = " AND ".join(f"l.{c} = g.{c}" for c in keys)
    differs = " OR ".join(f"l.{c} IS DISTINCT FROM g.{c}" for c in cols)
    L, G = _source(live), _source(golden)
    q = f"""
        WITH l AS (SELECT * FROM {L} x {where}), g AS (SELECT * FROM {G} x {where})
        SELECT (SELECT count(*) FROM l), (SELECT count(*) FROM g),
               (SELECT count(*) FROM g LEFT JOIN l ON {on} WHERE l.{keys[0]} IS NULL),
               (SELECT count(*) FROM l LEFT JOIN g ON {on} WHERE g.{keys[0]} IS NULL),
               (SELECT count(*) FROM l JOIN g ON {on} WHERE {differs})"""  # noqa: S608
    row = conn.execute(q, params).fetchone()  # SQL built from constants
    if row is None:  # pragma: no cover - aggregates always return one row
        raise RuntimeError(f"comparison query returned no row for {live}")
    lr, gr, missing, extra, wrong = (int(x) for x in row)
    sums = f"""
        SELECT {month_expr} AS m,
               sum(('x' || substr(md5(ROW({k}, {", ".join(cols)})::text), 1, 15))::bit(60)::bigint)
        FROM %s x {where} GROUP BY 1"""  # noqa: S608
    ls: dict[object, object] = dict(conn.execute(sums % L, params).fetchall())
    gs: dict[object, object] = dict(conn.execute(sums % G, params).fetchall())
    examples: list[dict[str, Any]] = []
    if missing or extra or wrong:
        ex = conn.execute(
            f"WITH l AS (SELECT * FROM {L} x {where}), g AS (SELECT * FROM {G} x {where})"  # noqa: S608
            f" SELECT {', '.join('coalesce(l.' + c + ', g.' + c + ')::text' for c in keys)},"
            f" CASE WHEN l.{keys[0]} IS NULL THEN 'missing' WHEN g.{keys[0]} IS NULL THEN 'extra'"
            f" ELSE 'wrong' END FROM l FULL JOIN g ON {on} WHERE l.{keys[0]} IS NULL"
            f" OR g.{keys[0]} IS NULL OR {differs} LIMIT 5",
            params,
        ).fetchall()
        examples = [dict(zip([*keys, "kind"], r, strict=True)) for r in ex]
    return TableDiff(live, lr, gr, missing, extra, wrong, ls == gs, examples)


def compare_all(conn: Conn, location_ids: list[int] | None = None) -> list[TableDiff]:
    """Compare every published table with its golden copy."""
    return [compare_table(conn, *c, location_ids=location_ids) for c in COMPARISONS]


def damage(diffs: list[TableDiff]) -> dict[str, int]:
    """Row-level damage summary used by the experiments (missing / extra / wrong)."""
    out: dict[str, int] = {}
    for d in diffs:
        name = d.table.split(".")[1]
        out[f"{name}_missing"] = d.missing
        out[f"{name}_extra"] = d.extra
        out[f"{name}_wrong"] = d.wrong
    return out
