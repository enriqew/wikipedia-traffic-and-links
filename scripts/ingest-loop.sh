#!/usr/bin/env bash
# Keep copying a month into HDFS while its download runs: one ingest pass every ten minutes, then a
# final pass once the download log says it is done, which writes the report.
#
#   WH_DATA_DIR=... scripts/ingest-loop.sh 2026-10
#
# Each pass only handles files the ledger in HDFS does not already hold, so the loop can be
# stopped and started again at any point (a machine restart included) without redoing work.
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
month=$1
data=${WH_DATA_DIR:?set WH_DATA_DIR to the data directory}
log="$data/ingest-$month.log"
until grep -q '^done:' "$data/download-$month.log" 2>/dev/null; do
  docker compose run --rm -T client python3 /scripts/ingest.py "$month" 2>/dev/null | grep -E '^(ok|MISMATCH) ' >> "$log"
  sleep 600
done
docker compose run --rm -T client python3 /scripts/ingest.py "$month" > "$data/ingest-$month.report.txt" 2>&1
status=$?
grep -E '^(ok|MISMATCH) ' "$data/ingest-$month.report.txt" >> "$log"
echo "final pass exit $status: $(grep -c '^ok ' "$log") ok, $(grep -c '^MISMATCH ' "$log") mismatched"
exit $status
