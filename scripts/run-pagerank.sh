#!/usr/bin/env bash
# PageRank on the cluster over a link graph built by run-graph.sh, then copy the ranks out.
#
#   WH_DATA_DIR=... scripts/run-pagerank.sh <wiki> <dump date YYYYMMDD> [reducers, default 3] [-D key=value ...]
#
# Options after the reducers go to the driver as they are (see wikihadoop.PageRank), and the log
# prints them. The driver refuses a work directory that already holds a run unless given one of:
#   -D wh.pagerank.resume=<iterations.json in HDFS, cut back to the iterations whose output survived>
#   -D wh.pagerank.overwrite=true
# An earlier jobs.log is kept beside the new one, renamed with the time it was last written.
#
# Reads /out/graph/<wiki>-<date>/graph/adjacency-r-*, works in /out/pagerank/<wiki>-<date>/ (the
# last two iterations, ranks/, iterations.json; see wikihadoop.PageRank). The log goes to
# $WH_DATA_DIR/pagerank/<wiki>-<date>/jobs.log, and ranks.tsv (page_id rank, in units of 10^-9 of
# an article's starting rank) and iterations.json beside it. Like run-window.sh, the run copies the
# jar when it starts; map output and the ranks are compressed, as in run-graph.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
wiki=$1 date=$2 reducers=${3:-3}
shift $(($# < 3 ? $# : 3))
options="$*"
data=${WH_DATA_DIR:?set WH_DATA_DIR to the data directory}
name="$wiki-$date"
local_out="$data/pagerank/$name"
mkdir -p "$local_out"
if [ -f "$local_out/jobs.log" ]; then
  mv "$local_out/jobs.log" "$local_out/jobs.$(date -r "$local_out/jobs.log" +%Y%m%d-%H%M%S).log"
fi
commit="$(git rev-parse HEAD)$(git diff --quiet HEAD -- mapreduce || echo ' (mapreduce has uncommitted changes)')"

docker compose run --rm -T client bash -s > "$local_out/jobs.log" 2>&1 <<EOF || true
set -e
z=org.apache.hadoop.io.compress.DefaultCodec
cp /jobs/wiki-hadoop.jar /tmp/run.jar
echo "commit $commit"
echo "jar sha256 \$(sha256sum /tmp/run.jar | cut -d' ' -f1)"
echo "driver options: -D wh.graph.reducers=$reducers $options"
echo "=== pagerank"
hadoop jar /tmp/run.jar wikihadoop.PageRank -D wh.graph.reducers=$reducers $options \
  -D mapreduce.map.output.compress=true -D mapreduce.map.output.compress.codec=\$z \
  -D mapreduce.output.fileoutputformat.compress=true -D mapreduce.output.fileoutputformat.compress.codec=\$z \
  "/out/graph/$name/graph/adjacency-r-*" /out/pagerank/$name
echo "=== done"
EOF
grep -E "^=== |^pagerank: |check failed|failed" "$local_out/jobs.log" || true
if ! grep -q "^=== done" "$local_out/jobs.log"; then
  echo "the run stopped early; see $local_out/jobs.log" >&2
  exit 1
fi
docker compose run --rm -T client python3 /scripts/hdfs_text.py "/out/pagerank/$name/ranks/part-m-*" \
  > "$local_out/ranks.tsv" 2>/dev/null
docker compose run --rm -T client hdfs dfs -cat "/out/pagerank/$name/iterations.json" \
  > "$local_out/iterations.json" 2>/dev/null
wc -l "$local_out/ranks.tsv" | sed "s|$local_out/||"
