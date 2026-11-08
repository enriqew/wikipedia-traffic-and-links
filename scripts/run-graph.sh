#!/usr/bin/env bash
# Build one wiki's link graph on the cluster from its dumps in HDFS, then copy the graph out.
#
#   WH_DATA_DIR=... scripts/run-graph.sh <wiki> <dump date YYYYMMDD> [reducers, default 3]
#
# Reads /raw/dumps/<wiki>/<date>/{page,linktarget,redirect,pagelinks}/ (scripts/ingest_dumps.py).
# Jobs, in order, each writing under /out/graph/<wiki>-<date>/ in HDFS (plan section 13):
#   link-targets   LinkTargets   what each title names (outputs targets, hops)
#   redirects      Redirects     link targets to a redirect take its target
#   edges          Edges         pagelinks rows against their resolved targets
#   graph          Adjacency     each article's distinct out-links (adjacency, titles, redirects)
# The first job that fails, or whose checks fail, stops the run. The log, counters included, goes
# to $WH_DATA_DIR/graphs/<wiki>-<date>/jobs.log; the outputs to graph.<output>.tsv and
# link-targets.hops.tsv beside it. Like run-window.sh, the run copies the jar when it starts.
# Map output and job output are compressed (zlib, native on the cluster): English Wikipedia's Edges
# shuffles about a billion records, and its output would take some 60 GB at replication 3.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
wiki=$1 date=$2 reducers=${3:-3}
data=${WH_DATA_DIR:?set WH_DATA_DIR to the data directory}
name="$wiki-$date"
local_out="$data/graphs/$name"
mkdir -p "$local_out"
commit="$(git rev-parse HEAD)$(git diff --quiet HEAD -- mapreduce || echo ' (mapreduce has uncommitted changes)')"

docker compose run --rm -T client bash -s > "$local_out/jobs.log" 2>&1 <<EOF || true
set -e
i=/raw/dumps/$wiki/$date
o=/out/graph/$name
z=org.apache.hadoop.io.compress.DefaultCodec
r="-D wh.graph.reducers=$reducers -D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=\$z -D mapreduce.output.fileoutputformat.compress=true -D mapreduce.output.fileoutputformat.compress.codec=\$z"
cp /jobs/wiki-hadoop.jar /tmp/run.jar
jar=/tmp/run.jar
echo "commit $commit"
echo "jar sha256 \$(sha256sum \$jar | cut -d' ' -f1)"
hdfs dfs -rm -r -f -skipTrash \$o >/dev/null 2>&1 || true
echo "=== link-targets"; hadoop jar \$jar wikihadoop.LinkTargets \$r \$o/link-targets \$i/page \$i/linktarget \$i/redirect
echo "=== redirects";    hadoop jar \$jar wikihadoop.Redirects \$r \$o/link-targets \$o/redirects
echo "=== edges";        hadoop jar \$jar wikihadoop.Edges \$r \$o/link-targets \$o/redirects \$o/edges \$i/pagelinks
echo "=== graph";        hadoop jar \$jar wikihadoop.Adjacency \$r \$o/edges \$o/graph \$i/page
echo "=== done"
EOF
grep -E "^=== |completed successfully|checks failed" "$local_out/jobs.log" | sed -E 's/^.*(Job job_[0-9_]+ completed successfully)/  \1/'
if ! grep -q "^=== done" "$local_out/jobs.log"; then
  echo "the run stopped early; see $local_out/jobs.log" >&2
  exit 1
fi

fetch() {  # $1 = job output directory, $2 = file prefix
  docker compose run --rm -T client python3 /scripts/hdfs_text.py "/out/graph/$name/$1/$2-r-*" \
    > "$local_out/$1.$2.tsv" 2>/dev/null
}
fetch graph adjacency
fetch graph titles
fetch graph redirects
fetch link-targets hops
wc -l "$local_out"/*.tsv | sed "s|$local_out/||"
