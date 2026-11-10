"""Provenance for every result file (rule 9): what ran, when, on which commit.

`git` is not installed in the app image, so the commit is read from .git directly.
Whether the working tree had uncommitted changes is NOT checked; the record says so.
"""

from __future__ import annotations

import json
import platform
import sys
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import Any


def git_commit(repo: Path = Path()) -> str:
    """Commit hash HEAD points to, or 'unknown'."""
    git = repo / ".git"
    try:
        head = (git / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref: "):
            return head
        ref = head.removeprefix("ref: ")
        ref_file = git / ref
        if ref_file.exists():
            return ref_file.read_text(encoding="utf-8").strip()
        packed = git / "packed-refs"
        if packed.exists():
            for line in packed.read_text(encoding="utf-8").splitlines():
                if line.endswith(" " + ref):
                    return line.split(" ", 1)[0]
    except OSError:
        pass
    return "unknown"


def record(
    command: str,
    outputs: list[str],
    extra: dict[str, Any] | None = None,
    log_file: Path = Path("results/provenance.jsonl"),
) -> dict[str, Any]:
    """Append one provenance line for a command that wrote `outputs`."""
    entry: dict[str, Any] = {
        "utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "command": command,
        "outputs": outputs,
        "git_commit": git_commit(),
        "working_tree_clean": "not checked",
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "packages": {p: version(p) for p in ("sentinel-aq", "polars", "pydantic")},
        **(extra or {}),
    }
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(entry, sort_keys=True) + "\n")
    return entry
