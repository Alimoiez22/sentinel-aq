"""M12 analysis: results.csv -> summary tables (with denominators) + SVG figures.

Every number written here is computed from experiments/results/results.csv (rule 4).
Small samples: 5 seeds per cell, so we report counts ("5/5"), medians and IQRs, and no
significance tests (RESEARCH §8).
"""

from __future__ import annotations

import html
import statistics
from pathlib import Path

import polars as pl

RESULTS = Path("experiments/results")
FIGURES = Path("docs/figures")
MODES = ["retries_only", "guarded"]
COLORS = {"retries_only": "#c0504d", "guarded": "#4f81bd"}


def _frac(k: int, n: int) -> str:
    return f"{k}/{n}"


def _med_iqr(xs: list[float]) -> str:
    if not xs:
        return "-"
    if len(xs) == 1:
        return f"{xs[0]:.3f}"
    q = statistics.quantiles(xs, n=4, method="inclusive")
    return f"{statistics.median(xs):.3f} [{q[0]:.3f}, {q[2]:.3f}]"


def damage_cols(df: pl.DataFrame) -> list[str]:
    return [c for c in df.columns if c.endswith(("_missing", "_extra", "_wrong"))]


def by_fault_mode(df: pl.DataFrame) -> pl.DataFrame:
    """One row per (fault, mode) with explicit k/n counts."""
    rows = []
    for (fault, mode), g in df.group_by(["fault", "mode"], maintain_order=True):
        n = g.height
        ttr = [x for x in g["time_to_verified_recovery_s"].to_list() if x is not None]
        rows.append({
            "fault": fault, "mode": mode, "n": n,
            "outputs_correct": _frac(int(g["outputs_correct"].sum()), n),
            "handled_as_expected": _frac(int(g["handled_as_expected"].sum()), n),
            "recovered_verified": _frac(int(g["recovered_verified"].sum()), n),
            "escalated": _frac(int(g["escalated"].sum()), n),
            "escalation_expected": bool(g["escalation_expected"][0]),
            "inappropriate_actions": int(g["inappropriate_actions"].sum()),
            "damaged_rows": int(g.select(pl.sum_horizontal(damage_cols(g))).sum().item()),
            "ttr_s_median_iqr": _med_iqr(ttr),
            "overhead_s_median_iqr": _med_iqr(
                [x for x in g["overhead_s"].to_list() if x is not None]),
        })  # fmt: skip
    order = {f"F{i}": i for i in range(16)}
    return pl.DataFrame(rows).sort([pl.col("fault").replace_strict(order), "mode"])


def by_mode(df: pl.DataFrame) -> pl.DataFrame:
    """Totals per mode, over faulty trials only (F0 is the overhead baseline)."""
    faulty = df.filter(pl.col("fault") != "F0")
    rows = []
    for mode in MODES:
        g = faulty.filter(pl.col("mode") == mode)
        n = g.height
        if not n:
            continue
        ttr = [x for x in g["time_to_verified_recovery_s"].to_list() if x is not None and x > 0]
        rows.append({
            "mode": mode, "trials": n,
            "outputs_correct": _frac(int(g["outputs_correct"].sum()), n),
            "handled_as_expected": _frac(int(g["handled_as_expected"].sum()), n),
            "recovered_verified": _frac(int(g["recovered_verified"].sum()), n),
            "escalated": _frac(int(g["escalated"].sum()), n),
            "trials_with_damage": _frac(int((g.select(pl.sum_horizontal(damage_cols(g)))
                                             .to_series() > 0).sum()), n),
            # Wrong MEASUREMENTS (hourly values or daily means) vs wrong monthly COUNTS:
            # a missing day makes the monthly summary understate complete days.
            "wrong_values_published": int(g.select(pl.sum_horizontal(
                "fact_measurement_hourly_wrong", "mart_daily_location_wrong")).sum().item()),
            "missing_location_days": int(g["mart_daily_location_missing"].sum()) // 2,
            "summary_rows_wrong": int(g["mart_exceedance_summary_wrong"].sum()),
            "inappropriate_actions": int(g["inappropriate_actions"].sum()),
            "ttr_s_median_iqr_when_needed": _med_iqr(ttr),
        })  # fmt: skip
    return pl.DataFrame(rows)


def markdown_table(df: pl.DataFrame) -> str:
    """GitHub-flavoured Markdown from a DataFrame (values as text)."""
    head = "| " + " | ".join(df.columns) + " |"
    sep = "|" + "---|" * len(df.columns)
    body = ["| " + " | ".join(str(v) for v in r) + " |" for r in df.iter_rows()]
    return "\n".join([head, sep, *body])


def svg_bars(df: pl.DataFrame, value_col: str, title: str, path: Path) -> Path:
    """Grouped bars per fault (one bar per mode) of a k/n column, as a share."""
    faults = df["fault"].unique(maintain_order=True).to_list()
    w, h, left, top, bh = 760, 320, 50, 40, 220
    gw = (w - left - 20) / len(faults)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
        f'font-family="sans-serif" font-size="11">',
        f'<text x="{left}" y="20" font-size="14">{html.escape(title)}</text>',
        f'<line x1="{left}" y1="{top + bh}" x2="{w - 10}" y2="{top + bh}" stroke="#333"/>',
    ]
    for tick in (0, 0.5, 1):
        y = top + bh - tick * bh
        parts.append(
            f'<text x="{left - 8}" y="{y + 4}" text-anchor="end">{int(tick * 100)}%</text>'
        )
        parts.append(f'<line x1="{left}" y1="{y}" x2="{w - 10}" y2="{y}" stroke="#ddd"/>')
    for i, f in enumerate(faults):
        for j, mode in enumerate(MODES):
            row = df.filter((pl.col("fault") == f) & (pl.col("mode") == mode))
            if not row.height:
                continue
            k, n = (int(x) for x in str(row[value_col][0]).split("/"))
            share = k / n if n else 0
            x = left + i * gw + 4 + j * (gw - 8) / 2
            bw = (gw - 8) / 2 - 2
            parts.append(
                f'<rect x="{x:.1f}" y="{top + bh - share * bh:.1f}" width="{bw:.1f}" '
                f'height="{share * bh:.1f}" fill="{COLORS[mode]}"><title>{f} {mode}: '
                f"{k}/{n}</title></rect>"
            )
        parts.append(
            f'<text x="{left + i * gw + gw / 2:.1f}" y="{top + bh + 15}" '
            f'text-anchor="middle">{f}</text>'
        )
    for j, mode in enumerate(MODES):
        parts.append(
            f'<rect x="{left + j * 140}" y="{h - 22}" width="12" height="12" '
            f'fill="{COLORS[mode]}"/><text x="{left + 18 + j * 140}" y="{h - 12}">{mode}</text>'
        )
    parts.append("</svg>")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts), encoding="utf-8")
    return path


def analyse(results_dir: Path = RESULTS, tag: str = "") -> list[Path]:
    """Write summary CSVs, the generated tables, figures and the README block.

    `tag` separates datasets: "" for synthetic (experiments/results), "real" for the
    real Lahore window (experiments/results_real). Each has its own README markers.
    """
    src = results_dir / "results.csv"
    df = pl.read_csv(src, infer_schema_length=None)
    fm, m = by_fault_mode(df), by_mode(df)
    out = [results_dir / "summary_by_fault_mode.csv", results_dir / "summary_by_mode.csv"]
    fm.write_csv(out[0])
    m.write_csv(out[1])
    md = results_dir / "results_table.md"
    md.write_text(
        f"<!-- generated by `make analyse` from {src.as_posix()}; do not edit -->\n"
        "### By mode (faulty trials F1-F15)\n\n" + markdown_table(m) + "\n\n"
        "### By fault and mode\n\n" + markdown_table(fm.drop("escalation_expected")) + "\n",
        encoding="utf-8",
    )
    out.append(md)
    suffix = f"_{tag}" if tag else ""
    label = " (real Lahore data)" if tag == "real" else " (synthetic data)"
    out.append(
        svg_bars(
            fm,
            "handled_as_expected",
            "Handled as expected (correct outputs + correct escalation)" + label,
            FIGURES / f"handled_as_expected{suffix}.svg",
        )
    )
    out.append(
        svg_bars(
            fm,
            "outputs_correct",
            "Published outputs equal golden" + label,
            FIGURES / f"outputs_correct{suffix}.svg",
        )
    )
    marker = f"results-{tag}" if tag else "results"
    inject_readme(m, Path("README.md"), src, md, marker=marker)
    return out


def inject_readme(
    by_mode_df: pl.DataFrame,
    readme: Path,
    src: Path = RESULTS / "results.csv",
    table: Path = RESULTS / "results_table.md",
    marker: str = "results",
) -> None:
    """Replace the README block between <!-- {marker}:start/end --> with the table."""
    start, end = f"<!-- {marker}:start -->", f"<!-- {marker}:end -->"
    text = readme.read_text(encoding="utf-8")
    if start not in text or end not in text:
        raise ValueError(f"README.md lacks the {marker} markers")
    block = (
        f"Generated by `make analyse` from `{src.as_posix()}` (faulty trials F1-F15; "
        "k/n = trials; times in seconds, median [IQR]).\n\n"
        + markdown_table(by_mode_df)
        + f"\n\nPer fault: [{table.as_posix()}]({table.as_posix()})."
    )
    head, rest = text.split(start, 1)
    _, tail = rest.split(end, 1)
    readme.write_text(f"{head}{start}\n{block}\n{end}{tail}", encoding="utf-8")
