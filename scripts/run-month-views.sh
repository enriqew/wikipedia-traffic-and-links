#!/usr/bin/env bash
# Each title's views over a window for the wikis given, on the cluster (MonthViews, plan section 15).
#
#   WH_DATA_DIR=... scripts/run-month-views.sh <run name> <first day YYYY-MM-DD> <days> <wiki,...> [reducers, default 3]
#
# Reads the window's user and automated files in /raw/pageviews/ and writes
# /out/<run>/month-views/views-r-* (wiki title user automated), compressed. The log, counters
# included, goes to $WH_DATA_DIR/runs/<run>/month-views.log; run-linked.sh reads the output in HDFS.
# Like run-window.sh, the run copies the jar when it starts and the log opens with the commit.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
run=$1 start=$2 days=$3 wikis=$4 reducers=${5:-3}
data=${WH_DATA_DIR:?set WH_DATA_DIR to the data directory}
local_out="$data/runs/$run"
mkdir -p "$local_out"
log="$local_out/month-views.log"
if [ -f "$log" ]; then
  mv "$log" "$local_out/month-views.$(date -r "$log" +%Y%m%d-%H%M%S).log"
fi

dates=$(python - "$start" "$days" <<'EOF'
import sys
from datetime import date, timedelta
start, days = date.fromisoformat(sys.argv[1]), int(sys.argv[2])
print(" ".join((start + timedelta(days=i)).isoformat() for i in range(days)))
EOF
)
inputs=""
for d in $dates; do
  inputs="$inputs /raw/pageviews/agent=user/date=$d /raw/pageviews/agent=automated/date=$d"
done
commit="$(git rev-parse HEAD)$(git diff --quiet HEAD -- mapreduce || echo ' (mapreduce has uncommitted changes)')"

docker compose run --rm -T client bash -s > "$log" 2>&1 <<EOF || true
set -e
o=/out/$run/month-views
z=org.apache.hadoop.io.compress.DefaultCodec
cp /jobs/wiki-hadoop.jar /tmp/run.jar
echo "commit $commit"
echo "jar sha256 \$(sha256sum /tmp/run.jar | cut -d' ' -f1)"
echo "window $start, $days days; wikis $wikis"
hdfs dfs -rm -r -f -skipTrash \$o >/dev/null 2>&1 || true
echo "=== month-views"
hadoop jar /tmp/run.jar wikihadoop.MonthViews -D wh.views.wikis=$wikis -D wh.graph.reducers=$reducers \
  -D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=\$z \
  -D mapreduce.output.fileoutputformat.compress=true -D mapreduce.output.fileoutputformat.compress.codec=\$z \
  \$o $inputs
echo "=== done"
EOF
grep -E "^=== |completed successfully|check failed" "$log" | sed -E 's/^.*(Job job_[0-9_]+ completed successfully)/  \1/'
if ! grep -q "^=== done" "$log"; then
  echo "the run stopped early; see $log" >&2
  exit 1
fi
