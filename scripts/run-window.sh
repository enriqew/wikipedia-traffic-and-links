#!/usr/bin/env bash
# Run the whole pageview pipeline over a window on the cluster, then copy its outputs out of HDFS.
#
#   WH_DATA_DIR=... scripts/run-window.sh <run name> <first day YYYY-MM-DD> <days>
#
# Jobs, in order, each writing under /out/<run>/ in HDFS:
#   domain-hour-user, domain-hour-automated   DomainHourTotals per agent
#   candidates                                SpikeCandidates (pass one)
#   article-days                              ArticleDays (pass two)
#   spike-hours                               SpikeHours (pass three)
#   top-spikes                                TopSpikes
# The first job that fails stops the run. The job logs, counters included, go to
# $WH_DATA_DIR/runs/<run>/jobs.log, and each output to $WH_DATA_DIR/runs/<run>/<job>.<output>.tsv.
# The run copies the jar when it starts and uses the copy, so a rebuild meanwhile does not reach
# its later jobs; the log opens with the commit and the copy's sha256.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
run=$1 start=$2 days=$3
data=${WH_DATA_DIR:?set WH_DATA_DIR to the data directory}
local_out="$data/runs/$run"
mkdir -p "$local_out"

dates=$(python - "$start" "$days" <<'EOF'
import sys
from datetime import date, timedelta
start, days = date.fromisoformat(sys.argv[1]), int(sys.argv[2])
print(" ".join((start + timedelta(days=i)).isoformat() for i in range(days)))
EOF
)
user="" automated=""
for d in $dates; do
  user="$user /raw/pageviews/agent=user/date=$d"
  automated="$automated /raw/pageviews/agent=automated/date=$d"
done
commit="$(git rev-parse HEAD)$(git diff --quiet HEAD -- mapreduce || echo ' (mapreduce has uncommitted changes)')"

docker compose run --rm -T client bash -s > "$local_out/jobs.log" 2>&1 <<EOF || true
set -e
o=/out/$run
w="-D wh.window.start=$start -D wh.window.days=$days"
cp /jobs/wiki-hadoop.jar /tmp/run.jar
jar=/tmp/run.jar
echo "commit $commit"
echo "jar sha256 \$(sha256sum \$jar | cut -d' ' -f1)"
hdfs dfs -rm -r -f -skipTrash \$o >/dev/null 2>&1 || true
echo "=== domain-hour-user";      hadoop jar \$jar wikihadoop.DomainHourTotals $user \$o/domain-hour-user
echo "=== domain-hour-automated"; hadoop jar \$jar wikihadoop.DomainHourTotals $automated \$o/domain-hour-automated
echo "=== candidates";   hadoop jar \$jar wikihadoop.SpikeCandidates \$w \$o/domain-hour-user \$o/candidates $user $automated
echo "=== article-days"; hadoop jar \$jar wikihadoop.ArticleDays \$w \$o/domain-hour-user \$o/candidates \$o/article-days $user $automated
echo "=== spike-hours";  hadoop jar \$jar wikihadoop.SpikeHours \$w \$o/domain-hour-user \$o/candidates \$o/article-days \$o/spike-hours $user
echo "=== top-spikes";   hadoop jar \$jar wikihadoop.TopSpikes \$o/spike-hours \$o/top-spikes
echo "=== done"
EOF
grep -E "^=== |completed successfully" "$local_out/jobs.log" | sed -E 's/^.*(Job job_[0-9_]+ completed successfully)/  \1/'
if ! grep -q "^=== done" "$local_out/jobs.log"; then
  echo "the run stopped early; see $local_out/jobs.log" >&2
  exit 1
fi

fetch() {  # $1 = job output directory, $2 = file prefix (part, candidates, totals, ...)
  docker compose run --rm -T client hdfs dfs -cat "/out/$run/$1/$2-r-*" > "$local_out/$1.$2.tsv" 2>/dev/null
}
fetch domain-hour-user part
fetch domain-hour-automated part
fetch candidates candidates
fetch candidates totals
fetch article-days spikes
fetch article-days articles
fetch spike-hours part
fetch top-spikes part
wc -l "$local_out"/*.tsv | sed "s|$local_out/||"
