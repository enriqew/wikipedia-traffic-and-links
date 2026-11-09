#!/usr/bin/env bash
# One wiki's views set against its PageRank, on the cluster (plan section 15), then copy it out.
#
#   WH_DATA_DIR=... scripts/run-linked.sh <wiki> <dump date YYYYMMDD> <views run> [reducers, default 3]
#
# <wiki> is the dump's name (enwiki); its pageview code is derived from it (en.wikipedia). Reads
# the graph in /out/graph/<wiki>-<date>/ (run-graph.sh), the ranks in /out/pagerank/<wiki>-<date>/
# (run-pagerank.sh) and the views in /out/<views run>/month-views/ (run-month-views.sh). Jobs, in
# order, each writing under /out/linked/<wiki>-<date>/ in HDFS:
#   title-targets  TitleTargets  the article each title of namespace 0 names
#   view-targets   ViewTargets   each viewed title's views to its article or a fate
#   linked         LinkedVsRead  every article: page_id title rank user automated user_through_redirects
# The log, counters included, goes to $WH_DATA_DIR/linked/<wiki>-<date>/jobs.log (an earlier one is
# kept, renamed with the time it was last written), and the table to linked.tsv beside it.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
wiki=$1 date=$2 views=$3 reducers=${4:-3}
data=${WH_DATA_DIR:?set WH_DATA_DIR to the data directory}
name="$wiki-$date"
code="${wiki%wiki}.wikipedia"
local_out="$data/linked/$name"
mkdir -p "$local_out"
if [ -f "$local_out/jobs.log" ]; then
  mv "$local_out/jobs.log" "$local_out/jobs.$(date -r "$local_out/jobs.log" +%Y%m%d-%H%M%S).log"
fi
commit="$(git rev-parse HEAD)$(git diff --quiet HEAD -- mapreduce || echo ' (mapreduce has uncommitted changes)')"

docker compose run --rm -T client bash -s > "$local_out/jobs.log" 2>&1 <<EOF || true
set -e
g=/out/graph/$name
o=/out/linked/$name
z=org.apache.hadoop.io.compress.DefaultCodec
r="-D wh.graph.reducers=$reducers -D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=\$z -D mapreduce.output.fileoutputformat.compress=true -D mapreduce.output.fileoutputformat.compress.codec=\$z"
cp /jobs/wiki-hadoop.jar /tmp/run.jar
jar=/tmp/run.jar
echo "commit $commit"
echo "jar sha256 \$(sha256sum \$jar | cut -d' ' -f1)"
echo "wiki $code; graph \$g; ranks /out/pagerank/$name/ranks; views /out/$views/month-views"
hdfs dfs -rm -r -f -skipTrash \$o >/dev/null 2>&1 || true
echo "=== title-targets"; hadoop jar \$jar wikihadoop.TitleTargets \$r \$g/graph \$g/link-targets \$o/title-targets
echo "=== view-targets";  hadoop jar \$jar wikihadoop.ViewTargets \$r -D wh.views.wiki=$code /out/$views/month-views \$o/title-targets \$o/view-targets
echo "=== linked";        hadoop jar \$jar wikihadoop.LinkedVsRead \$r /out/pagerank/$name/ranks \$g/graph \$o/view-targets \$o/linked
echo "=== done"
EOF
grep -E "^=== |completed successfully|check failed" "$local_out/jobs.log" | sed -E 's/^.*(Job job_[0-9_]+ completed successfully)/  \1/'
if ! grep -q "^=== done" "$local_out/jobs.log"; then
  echo "the run stopped early; see $local_out/jobs.log" >&2
  exit 1
fi
docker compose run --rm -T client python3 /scripts/hdfs_text.py "/out/linked/$name/linked/linked-r-*" \
  > "$local_out/linked.tsv" 2>/dev/null
wc -l "$local_out/linked.tsv" | sed "s|$local_out/||"
