#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: ./darshan_to_json.sh <file.darshan>"
  exit 1
fi

DARSHAN_FILE="$1"

if [ ! -f "$DARSHAN_FILE" ]; then
  echo "Error: file not found: $DARSHAN_FILE"
  exit 1
fi

BASENAME="$(basename "$DARSHAN_FILE" .darshan)"

echo "[1/2] Generating job.csv from job_stats..."
python3 -m darshan job_stats "$DARSHAN_FILE" --csv > job.csv

if [ ! -s job.csv ]; then
  echo "Error: job.csv is empty"
  exit 1
fi

echo "[2/2] Running darshan_to_json.py..."
python3 darshan_to_json.py "$DARSHAN_FILE"

echo "[OK] Done."
echo "  - job.csv"
echo "  - ${BASENAME}.json"
