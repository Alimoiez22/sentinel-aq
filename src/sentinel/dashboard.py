"""Static HTML dashboard over the ops views (M12, ADR-018). One self-contained file."""

from __future__ import annotations

import html
from datetime import UTC, datetime
from pathlib import Path

from sentinel.pipeline import Conn

SECTIONS = [
    ("Publication status", "SELECT * FROM ops.v_publication ORDER BY status"),
    ("Incidents and decisions (latest 50)",
     "SELECT incident_id, detected_at, location_id, source_date, failure_class, decision,"
     " policy_rule_id, action, attempts, outcome, escalated, time_to_verified_recovery_s, origin"
     " FROM ops.v_incidents ORDER BY incident_id DESC LIMIT 50"),
    ("Recovery times by failure class", "SELECT * FROM ops.v_recovery_times ORDER BY 1"),
    ("Quality rule hits", "SELECT * FROM ops.v_reject_rates ORDER BY rule_id"),
    ("Revision history", "SELECT * FROM ops.v_revisions ORDER BY source_date DESC, location_id"),
    ("Batch timeline (latest 50)",
     "SELECT batch_id, location_id, source_date, revision_no, status, header_variant,"
     " raw_row_count, arrival_ts, stages_done FROM ops.v_batch_timeline"
     " ORDER BY batch_id DESC LIMIT 50"),
]  # fmt: skip


def render(conn: Conn, title: str) -> str:
    """HTML page with one table per section; numbers come straight from the views."""
    db = conn.execute("SELECT current_database()").fetchone()
    parts = [
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        f"<title>{html.escape(title)}</title><style>",
        ":root{--bg:#fff;--fg:#1d1d1f;--mut:#666;--line:#ddd}",
        "@media (prefers-color-scheme:dark){:root{--bg:#141414;--fg:#eee;--mut:#aaa;--line:#333}}",
        "body{font:14px system-ui,sans-serif;margin:16px;background:var(--bg);color:var(--fg)}",
        "table{border-collapse:collapse;margin:8px 0 24px;display:block;overflow-x:auto}",
        "td,th{border:1px solid var(--line);padding:4px 8px;text-align:left;white-space:nowrap}",
        "th{background:rgba(127,127,127,.12)}p.m{color:var(--mut)}</style></head><body>",
        f"<h1>{html.escape(title)}</h1>",
        f"<p class='m'>Database <code>{html.escape(str(db[0] if db else '?'))}</code>, generated "
        f"{datetime.now(UTC).isoformat(timespec='seconds')} by <code>make dashboard</code>.</p>",
    ]
    for name, sql in SECTIONS:
        cur = conn.execute(sql)  # constant SQL from this module
        cols = [d.name for d in cur.description or []]
        rows = cur.fetchall()
        parts.append(f"<h2>{html.escape(name)}</h2>")
        if not rows:
            parts.append("<p class='m'>No rows.</p>")
            continue
        parts.append("<table><tr>" + "".join(f"<th>{html.escape(c)}</th>" for c in cols) + "</tr>")
        for r in rows:
            parts.append(
                "<tr>"
                + "".join(f"<td>{html.escape('' if v is None else str(v))}</td>" for v in r)
                + "</tr>"
            )
        parts.append("</table>")
    parts.append("</body></html>")
    return "\n".join(parts)


def write(conn: Conn, out: Path, title: str = "Sentinel-AQ operations") -> Path:
    """Render and save the dashboard."""
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(conn, title), encoding="utf-8")
    return out
