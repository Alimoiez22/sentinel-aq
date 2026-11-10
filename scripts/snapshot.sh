#!/usr/bin/env bash
# Daily Track B snapshot. Meant to be run by Windows Task Scheduler through Git Bash:
#   "C:\Program Files\Git\bin\bash.exe" -lc "/d/projects/sentinel-aq/scripts/snapshot.sh"
# Every run appends one line to data/snapshots/_runs.log, success or failure, so missed
# days are visible (gaps are reported, never filled).
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
mkdir -p data/snapshots
log=data/snapshots/_runs.log
started=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if ! docker info >/dev/null 2>&1; then
  echo "$started FAILED docker-not-running" >>"$log"
  exit 1
fi

if make snapshot >>data/snapshots/_last_run.out 2>&1; then
  echo "$started OK" >>"$log"
else
  echo "$started FAILED make-snapshot (see data/snapshots/_last_run.out)" >>"$log"
  exit 1
fi
