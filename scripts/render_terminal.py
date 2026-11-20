"""Render captured terminal output as an SVG "screenshot" for the README.

The image contains exactly the captured text (no editing), plus a title bar naming the
command and the capture date, so a reader can reproduce it by running that command.

Usage: python scripts/render_terminal.py <text file> <out.svg> "<command>" "<date>"
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

CHAR_W, LINE_H, PAD, BAR = 7.8, 17, 16, 30
COLORS = {"RESULT:": "#7ee787", "OUTPUTS DIFFER": "#ff7b72"}


def render(text: str, command: str, captured: str) -> str:
    """SVG markup for the given text."""
    lines = text.rstrip("\n").split("\n")
    width = int(max(len(line) for line in lines) * CHAR_W + 2 * PAD)
    height = len(lines) * LINE_H + BAR + 2 * PAD
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'font-family="Consolas, Menlo, monospace" font-size="13">',
        f'<rect width="{width}" height="{height}" rx="8" fill="#0d1117"/>',
        f'<rect width="{width}" height="{BAR}" rx="8" fill="#161b22"/>',
        *(f'<circle cx="{16 + i * 18}" cy="15" r="5" fill="{c}"/>'
          for i, c in enumerate(("#ff5f56", "#ffbd2e", "#27c93f"))),
        f'<text x="80" y="20" fill="#8b949e">$ {html.escape(command)}   '
        f"(real output, captured {html.escape(captured)})</text>",
    ]  # fmt: skip
    for i, line in enumerate(lines):
        color = next((c for k, c in COLORS.items() if line.startswith(k)), "#c9d1d9")
        y = BAR + PAD + (i + 1) * LINE_H - 4
        out.append(f'<text x="{PAD}" y="{y}" fill="{color}" xml:space="preserve">'
                   f"{html.escape(line)}</text>")  # fmt: skip
    out.append("</svg>")
    return "\n".join(out)


def main() -> None:
    """Command-line entry point."""
    src, dst, command, captured = sys.argv[1:5]
    Path(dst).write_text(render(Path(src).read_text(encoding="utf-8"), command, captured),
                         encoding="utf-8")  # fmt: skip
    print(dst)


if __name__ == "__main__":
    main()
