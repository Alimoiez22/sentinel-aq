#!/usr/bin/env bash
# Download the archive files of chosen locations (wrapper kept because the brief asks
# for it; the work is done by `make fetch`, inside Docker).
#   scripts/fetch_openaq.sh "123 456"          # print the plan (sizes), download nothing
#   scripts/fetch_openaq.sh "123 456" yes      # download
set -euo pipefail
cd "$(dirname "$0")/.."
if [ $# -lt 1 ]; then
  echo "usage: $0 \"<location ids>\" [yes]" >&2
  exit 2
fi
make fetch LOCATIONS="$1" CONFIRM="${2:-no}"
