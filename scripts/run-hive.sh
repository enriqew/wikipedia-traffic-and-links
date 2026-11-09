#!/usr/bin/env bash
# Hive over a window's and a dump's job outputs (plan section 16), then copy its results out.
#
#   WH_DATA_DIR=... scripts/run-hive.sh <run> <dump date YYYYMMDD>
#
# Starts HiveServer2 (the hive service in compose.yaml) if it is not up, and puts Tez's own jars in
# HDFS the first time, without the Hadoop 3.3.6 jars Tez ships, so that its containers use the
# cluster's. Then rebuilds the table definitions (hive/tables.sql) and runs hive/results.sql, whose
# every query writes its result to /out/hive/<run>-<dump>/<name>/ in HDFS. The log goes to
# $WH_DATA_DIR/hive/<run>-<dump>/hive.log and each result to <name>.tsv beside it.
#
# The ranked wikis come from wikis.tsv as it was at the commit the window ran (the commit its log
# opens with), the table the jobs ranked by; it goes to /raw/wikis/ in HDFS for the tables. With
# Simple English they are the wikis whose daily and hourly views the queries read.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
run=$1 dump=$2
data=${WH_DATA_DIR:?set WH_DATA_DIR to the data directory}
name="$run-$dump"
local_out="$data/hive/$name"
results=/out/hive/$name
mkdir -p "$local_out"

window_commit=$(sed -n 's/^commit \([0-9a-f]*\).*/\1/p' "$data/runs/$run/jobs.log" | head -1)
wikis_tsv=$(git show "$window_commit:mapreduce/src/main/resources/wikis.tsv")
quote() { sed "s/.*/'&'/" | paste -sd, -; }
ranked=$(cut -f1 <<<"$wikis_tsv" | quote)
wikis=$( (cut -f1 <<<"$wikis_tsv"; echo simple.wikipedia) | quote)
domains=$( (cut -f1 <<<"$wikis_tsv"; echo simple.wikipedia) \
  | sed 's/\.wikipedia$//' | while read -r c; do echo "$c"; echo "$c.m"; done | quote)

docker compose --profile hive up -d hive 2>/dev/null
hive() { docker compose exec -T hive "$@"; }
beeline() {
  hive beeline -u jdbc:hive2://localhost:10000/ -n hadoop --hiveconf hive.server2.in.place.progress=false \
    --hivevar run="$run" --hivevar dump="$dump" \
    --hivevar ranked="$ranked" --hivevar wikis="$wikis" --hivevar domains="$domains" \
    --hivevar results="$results" "$@"
}
for _ in $(seq 1 60); do
  hive beeline -u jdbc:hive2://localhost:10000/ -n hadoop -e "SHOW DATABASES;" 2>/dev/null | grep -q default && break
  sleep 5
done

hive bash -s <<'EOF' 2>&1 | { grep -v SLF4J || true; }
t=/apps/tez-0.10.4
if ! hdfs dfs -test -e /apps/_tez-0.10.4-complete; then
  hdfs dfs -rm -r -f -skipTrash $t >/dev/null
  hdfs dfs -mkdir -p $t/lib
  hdfs dfs -put /opt/tez/*.jar $t/
  hdfs dfs -put $(ls /opt/tez/lib/*.jar | grep -v '/hadoop-') $t/lib/
  hdfs dfs -touchz /apps/_tez-0.10.4-complete
  echo "Tez 0.10.4 put in $t"
fi
EOF

printf '%s\n' "$wikis_tsv" | hive bash -c 'hdfs dfs -mkdir -p /raw/wikis && hdfs dfs -put -f - /raw/wikis/wikis.tsv' 2>&1 \
  | { grep -v SLF4J || true; }

commit="$(git rev-parse HEAD)$(git diff --quiet HEAD -- hive cluster/hive scripts/run-hive.sh \
  || echo ' (hive has uncommitted changes)')"
{
  echo "commit $commit"
  echo "wikis.tsv from $window_commit"
  echo "tables sha256 $(sha256sum hive/tables.sql | cut -d' ' -f1)"
  echo "results sha256 $(sha256sum hive/results.sql | cut -d' ' -f1)"
  echo "=== tables";  beeline -f /hive/tables.sql
  echo "=== results"; beeline -f /hive/results.sql
  echo "=== done"
} > "$local_out/hive.log" 2>&1 || true
if ! grep -q "^=== done" "$local_out/hive.log" || grep -qE "^Error:" "$local_out/hive.log"; then
  grep -E "^Error:" "$local_out/hive.log" | head -3 >&2
  echo "the run failed; see $local_out/hive.log" >&2
  exit 1
fi

rm -f "$local_out"/*.tsv
for q in $(hive hdfs dfs -ls -C "$results" 2>/dev/null | grep -v SLF4J); do
  hive bash -c "hdfs dfs -cat '$q/*' 2>/dev/null" > "$local_out/$(basename "$q").tsv"
done
grep -E "rows? (affected|selected) \(" "$local_out/hive.log" | sed -E 's/^.*\(([0-9.]+) seconds\).*/\1/' \
  | awk '{s+=$1} END {printf "%d statements, %.0f s\n", NR, s}'
wc -l "$local_out"/*.tsv | sed "s|$local_out/||"
