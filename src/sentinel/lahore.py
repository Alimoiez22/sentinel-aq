"""The analytical output (PROJECT_BRIEF §1): Lahore PM2.5 by month, from the VERIFIED marts.

Only published days count: daily means exist only for complete days (>= 18 h) that
were not blocked, and `make reconcile` has shown these marts equal the golden rebuild.
Exceedance = daily mean above the WHO 2021 24-hour guideline (15 µg/m³); the guideline is
a 99th-percentile statement, so these are "exceedance days", not breaches (RESEARCH §2.10).
"""

from __future__ import annotations

import html
from datetime import date
from pathlib import Path

import polars as pl

from sentinel.pipeline import Conn

OUT_CSV = Path("results/lahore_pm25_by_month.csv")
OUT_YEAR = Path("results/lahore_pm25_by_season.csv")
OUT_MD = Path("results/lahore_pm25.md")
OUT_SVG = Path("docs/figures/lahore_pm25_monthly.svg")

MONTHLY_SQL = """
SELECT m.location_id, l.name, date_trunc('month', m.local_date)::date AS month,
       count(*) AS days_gated,
       count(m.daily_mean) AS complete_days,
       round(avg(m.daily_mean)::numeric, 1) AS mean_of_daily_means,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY m.daily_mean))::numeric, 1)
         AS median_daily_mean,
       count(*) FILTER (WHERE m.exceeds_guideline) AS exceedance_days
FROM wh.mart_daily_location m JOIN wh.dim_location l USING (location_id)
WHERE m.parameter = 'pm25'
GROUP BY 1, 2, 3 ORDER BY 1, 3
"""

SEASON_SQL = """
SELECT m.location_id,
       CASE WHEN extract(month FROM m.local_date) IN (11, 12)
            THEN extract(year FROM m.local_date)::int
            ELSE extract(year FROM m.local_date)::int - 1 END AS winter_starting,
       count(m.daily_mean) AS complete_days,
       round(avg(m.daily_mean)::numeric, 1) AS mean_of_daily_means,
       count(*) FILTER (WHERE m.exceeds_guideline) AS exceedance_days
FROM wh.mart_daily_location m
WHERE m.parameter = 'pm25' AND extract(month FROM m.local_date) IN (11, 12, 1, 2)
GROUP BY 1, 2 ORDER BY 1, 2
"""


def frame(conn: Conn, sql: str) -> pl.DataFrame:
    cur = conn.execute(sql)  # constant SQL from this module
    return pl.DataFrame(cur.fetchall(), schema=[d.name for d in cur.description or []],
                        orient="row", infer_schema_length=None)  # fmt: skip


def _polyline(points: list[str], color: str) -> str:
    """A line for 2+ points; a dot for an isolated month."""
    if len(points) == 1:
        cx, cy = points[0].split(",")
        return f'<circle cx="{cx}" cy="{cy}" r="2.2" fill="{color}"/>'
    return (
        f'<polyline fill="none" stroke="{color}" stroke-width="1.6" points="{" ".join(points)}"/>'
    )


def svg_monthly(df: pl.DataFrame, path: Path, guideline: float = 15.0) -> Path:
    """Monthly median of daily means per location, with the WHO 24-h guideline line."""
    w, h, left, right, top, bottom = 760, 340, 56, 16, 36, 48
    d = df.filter(pl.col("median_daily_mean").is_not_null()).with_columns(
        pl.col("median_daily_mean").cast(pl.Float64)
    )
    months = sorted(set(d["month"].to_list()))
    if not months:
        raise ValueError("no complete days to plot")
    ymax = max(float(d["median_daily_mean"].max() or 0.0), guideline) * 1.08  # type: ignore[arg-type]
    span = (months[-1] - months[0]).days or 1

    def x(m: date) -> float:
        return float(left + (m - months[0]).days / span * (w - left - right))

    def y(v: float) -> float:
        return top + (1 - v / ymax) * (h - top - bottom)

    colors = ["#2f6db3", "#c0504d", "#4f8f3a"]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        'font-family="sans-serif" font-size="11">',
        f'<text x="{left}" y="20" font-size="14">Lahore PM2.5: monthly median of daily '
        "means (published, complete days only; gaps = no complete day)</text>",
        f'<text x="14" y="{top + (h - top - bottom) / 2:.0f}" '
        f'transform="rotate(-90 14 {top + (h - top - bottom) / 2:.0f})" '
        'text-anchor="middle">PM2.5, µg/m³</text>',
    ]
    for t in range(0, int(ymax) + 1, 50):
        parts.append(
            f'<line x1="{left}" x2="{w - right}" y1="{y(t):.1f}" y2="{y(t):.1f}" '
            f'stroke="#e3e3e3"/><text x="{left - 6}" y="{y(t) + 4:.1f}" '
            f'text-anchor="end">{t}</text>'
        )
    gy = y(guideline)
    parts.append(
        f'<line x1="{left}" x2="{w - right}" y1="{gy:.1f}" y2="{gy:.1f}" '
        'stroke="#555" stroke-dasharray="4 3"/>'
        f'<text x="{w - right}" y="{gy - 4:.1f}" text-anchor="end">WHO 24-h guideline '
        f"{guideline:g} µg/m³</text>"
    )
    years = sorted({m.year for m in months})
    for yr in years:
        jan = [m for m in months if m.year == yr]
        parts.append(f'<text x="{x(jan[0]):.1f}" y="{h - bottom + 16}">{yr}</text>')
    for i, ((loc, name), g) in enumerate(d.group_by(["location_id", "name"], maintain_order=True)):
        c = colors[i % len(colors)]
        # One polyline per run of consecutive months: a month without complete days is a
        # GAP in the line, never an interpolated segment.
        segment: list[str] = []
        prev: date | None = None
        for m, v in zip(g["month"].to_list(), g["median_daily_mean"].to_list(), strict=True):
            consecutive = prev is not None and (m.year * 12 + m.month) - (
                prev.year * 12 + prev.month) == 1  # fmt: skip
            if segment and not consecutive:
                parts.append(_polyline(segment, c))
                segment = []
            segment.append(f"{x(m):.1f},{y(v):.1f}")
            prev = m
        if segment:
            parts.append(_polyline(segment, c))
        parts.append(
            f'<rect x="{left + i * 300}" y="{h - 18}" width="12" height="12" fill="{c}"/>'
            f'<text x="{left + 18 + i * 300}" y="{h - 8}">{html.escape(str(name))} '
            f"({loc})</text>"
        )
    parts.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts), encoding="utf-8")
    return path


def report(conn: Conn) -> list[Path]:
    """CSV tables, a generated Markdown summary and the figure."""
    monthly, season = frame(conn, MONTHLY_SQL), frame(conn, SEASON_SQL)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    monthly.write_csv(OUT_CSV)
    season.write_csv(OUT_YEAR)
    from sentinel.analysis import markdown_table

    totals = monthly.group_by("location_id", "name").agg(
        pl.col("days_gated").sum(), pl.col("complete_days").sum(),
        pl.col("exceedance_days").sum()).sort("location_id")  # fmt: skip
    OUT_MD.write_text(
        "<!-- generated by `make lahore-report` from wh.mart_daily_location; do not edit -->\n"
        "# Lahore PM2.5 (published, verified marts)\n\n"
        "Exceedance day = complete day (>= 18 h) with daily mean > 15 µg/m³ (WHO 2021 24-h "
        "guideline, a 99th-percentile statement). Low-cost sensor values are uncorrected.\n\n"
        "## Totals\n\n" + markdown_table(totals) + "\n\n"
        "## Winters (Nov-Feb; labelled by the year the winter starts)\n\n"
        + markdown_table(season)
        + "\n",
        encoding="utf-8",
    )
    return [OUT_CSV, OUT_YEAR, OUT_MD, svg_monthly(monthly, OUT_SVG)]
