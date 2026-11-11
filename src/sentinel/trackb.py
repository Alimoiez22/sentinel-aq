"""Track B (M11): what the REAL archive did during our snapshot window.

Reads data/snapshots/*/{changes.csv,meta.json} and the raw store, and the live warehouse:
* revisions: count, lateness (hours between the end of the local day and the change
  being seen), and how many values changed (old vs new bytes, compared row by row);
* new files: lateness of first appearance;
* real quality-rule hits and real incidents (origin = 'real') with their outcomes.
Every count is reported with its observation window; zero is reported as zero.
"""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sentinel.contract import ContractError, parse_file
from sentinel.openaq.archive import parse_key
from sentinel.pipeline import Conn
from sentinel.rawstore import RawStore

SNAPSHOTS = Path("data/snapshots")
OUT_CSV = Path("results/trackb_changes.csv")
OUT_MD = Path("results/trackb_report.md")


def value_changes(store: RawStore, old_sha: str, new_sha: str) -> dict[str, int]:
    """Rows added / removed / changed between two versions of a day file."""
    try:
        old = parse_file(store.get(old_sha)).frame
        new = parse_file(store.get(new_sha)).frame
    except (ContractError, FileNotFoundError):
        return {"rows_added": -1, "rows_removed": -1, "values_changed": -1}

    def keyed(df: Any) -> dict[tuple[object, object, object], object]:
        return {(r["sensor_id"], r["datetime_raw"], r["parameter"]): r["value_raw"]
                for r in df.iter_rows(named=True)}  # fmt: skip

    o, n = keyed(old), keyed(new)
    return {"rows_added": len(n.keys() - o.keys()), "rows_removed": len(o.keys() - n.keys()),
            "values_changed": sum(o[k] != n[k] for k in o.keys() & n.keys())}  # fmt: skip


def collect_changes(root: Path = SNAPSHOTS, store: RawStore | None = None) -> list[dict[str, Any]]:
    """One row per change seen by any snapshot (the first snapshot's 'new' rows are the
    baseline inventory, not arrivals, and are excluded)."""
    store = store or RawStore(Path("data/raw"))
    runs = sorted(p for p in root.glob("*/meta.json") if not p.parent.name.startswith("."))
    rows: list[dict[str, Any]] = []
    for meta_path in runs:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("previous_run") is None:
            continue
        seen_at = datetime.strptime(meta["run_id"], "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        with (meta_path.parent / "changes.csv").open(encoding="utf-8") as f:
            for c in csv.DictReader(f):
                loc, day = parse_key(c["key"])
                day_end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
                row: dict[str, Any] = {
                    "run_id": meta["run_id"], "key": c["key"], "location_id": loc,
                    "source_date": day.isoformat(), "change": c["change"],
                    "lateness_h_upper_bound": round((seen_at - day_end).total_seconds() / 3600, 1),
                    "rows_added": "", "rows_removed": "", "values_changed": "",
                }  # fmt: skip
                if c["change"] == "changed" and c["old_sha256"] and c["new_sha256"]:
                    row.update(value_changes(store, c["old_sha256"], c["new_sha256"]))
                rows.append(row)
    return rows


def run_log_window(root: Path = SNAPSHOTS) -> dict[str, Any]:
    """Snapshot runs attempted / succeeded, from data/snapshots/_runs.log."""
    log = root / "_runs.log"
    lines = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    ok = [line.split()[0] for line in lines if " OK" in line]
    return {"attempted": len(lines), "succeeded": len(ok), "failed": len(lines) - len(ok),
            "first": ok[0] if ok else None, "last": ok[-1] if ok else None}  # fmt: skip


def live_quality(conn: Conn) -> list[tuple[Any, ...]]:
    """Real rule hits per rule (live warehouse; injected trials live in warehouse_exp)."""
    return conn.execute(
        "SELECT rule_id, action, count(DISTINCT batch_id), sum(n_rows) FROM ops.quality_stats"
        " WHERE n_rows > 0 GROUP BY rule_id, action ORDER BY rule_id"
    ).fetchall()


def live_incidents(conn: Conn) -> list[tuple[Any, ...]]:
    """Real incidents by class, decision and outcome."""
    return conn.execute(
        "SELECT failure_class, decision, coalesce(outcome, 'open'), count(*) FROM ops.incident"
        " WHERE origin = 'real' GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"
    ).fetchall()


def report(conn: Conn, root: Path = SNAPSHOTS) -> Path:
    """Write results/trackb_changes.csv and the generated Markdown report."""
    rows = collect_changes(root)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    cols = ["run_id", "key", "location_id", "source_date", "change", "lateness_h_upper_bound",
            "rows_added", "rows_removed", "values_changed"]  # fmt: skip
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    window = run_log_window(root)
    n_runs = len([p for p in root.glob("*/meta.json") if not p.parent.name.startswith(".")])
    by = {k: sum(r["change"] == k for r in rows) for k in ("new", "changed", "disappeared")}
    changed = [r for r in rows if r["change"] == "changed"]
    lines = [
        "# Track B: real archive behaviour (generated by `make revisions-report`)", "",
        f"Snapshot runs on disk: {n_runs}. Scheduled attempts in _runs.log: "
        f"{window['attempted']} ({window['succeeded']} OK, {window['failed']} failed); "
        f"first OK {window['first']}, last OK {window['last']}.", "",
        "| change type | count |", "|---|---|",
        *[f"| {k} | {v} |" for k, v in by.items()], "",
        "Lateness is an upper bound: snapshot time minus the end of the local day (UTC "
        "approximation), so it includes up to one day of snapshot interval.", "",
    ]  # fmt: skip
    if changed:
        lat = sorted(r["lateness_h_upper_bound"] for r in changed)
        vals = [r["values_changed"] for r in changed if isinstance(r["values_changed"], int)]
        lines += [
            f"Revisions: {len(changed)}; lateness median {lat[len(lat) // 2]} h, "
            f"max {lat[-1]} h; values changed per revision: "
            f"{', '.join(map(str, vals)) or 'not measured'}.",
            "",
        ]
    else:
        lines += ["Revisions observed: 0 in this window.", ""]
    q, inc = live_quality(conn), live_incidents(conn)
    lines += [
        "## Real quality-rule hits (live warehouse)",
        "",
        "| rule | action | batches | rows |",
        "|---|---|---|---|",
        *[f"| {a} | {b} | {c} | {d} |" for a, b, c, d in q],
    ]
    lines += ["", f"({len(q)} rules with hits; 0 means no real data processed yet.)", "",
              "## Real incidents (origin = real)", "", "| class | decision | outcome | count |",
              "|---|---|---|---|", *[f"| {a} | {b} | {c} | {d} |" for a, b, c, d in inc], "",
              f"({sum(r[3] for r in inc)} real incidents.)", ""]  # fmt: skip
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    return OUT_MD
