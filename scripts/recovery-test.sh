#!/usr/bin/env bash
# Determinism and recovery for DomainHourTotals.
#
# Five runs over the same input, each compared with the first by the sha256 of its output:
#   clean        an undisturbed run
#   rerun        the same job again
#   worker       a whole worker killed mid-job (DataNode, NodeManager and its task JVMs: a node failure)
#   datanode     only a worker's DataNode killed mid-job (its blocks still have two other replicas)
#   nodemanager  only a worker's NodeManager killed mid-job
# The victim is the worker running the most containers at the moment of the kill, and the kill
# lands once the map phase passes KILL_AT (default 30%). After each failure the daemon is brought
# back and the script waits for three live DataNodes, three NodeManagers and no under-replicated
# block before the next run.
#
#   scripts/recovery-test.sh [input dir]...
# Default input: the 1 and 28 October 2026 user files (eleven blocks, so the maps run in two waves).
# Prints one line per run and a JSON summary; exits non-zero if any output differs from the clean run.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
KILL_AT=${KILL_AT:-0.30}
if [ $# -eq 0 ]; then
  set -- /raw/pageviews/agent=user/date=2026-10-01 /raw/pageviews/agent=user/date=2026-10-28
fi
INPUTS="$*"

client() { docker compose run --rm -T client "$@" 2>/dev/null; }

wait_healthy() {
  client bash -c '
    for i in $(seq 1 120); do
      n=$(yarn node -list 2>/dev/null | grep -c RUNNING || true)
      l=$(hdfs dfsadmin -report 2>/dev/null | grep -oP "Live datanodes \(\K[0-9]+" || true)
      u=$(hdfs fsck /raw 2>/dev/null | grep -oP "Under-replicated blocks:\s*\K[0-9]+" || true)
      [ "$n" = 3 ] && [ "$l" = 3 ] && [ "$u" = 0 ] && exit 0
      sleep 5
    done
    echo "cluster not healthy: nodemanagers=$n datanodes=$l under_replicated=$u" >&2; exit 1'
}

# The worker with the most running containers right now.
busiest_worker() {
  client yarn node -list 2>/dev/null | awk '/RUNNING/ {split($1, h, ":"); print $NF, h[1]}' | sort -rn | head -1 | awk '{print $2}'
}

map_progress() {  # $1 = container name of the job client, prints the map completion (0..1) or nothing
  local job
  job=$(docker logs "$1" 2>&1 | grep -oP 'Running job: \Kjob_[0-9_]+' | head -1 || true)
  [ -n "$job" ] || return 0
  docker exec "$1" mapred job -status "$job" 2>/dev/null | grep -oP 'map\(\) completion: \K[0-9.]+' || true
}

run() {  # $1 = run name, $2 = what to kill (none|worker|datanode|nodemanager)
  local name=$1 kill=$2 out=/out/recovery/$1 c=wh-recovery-$1 victim=- killed_at=-
  client hdfs dfs -rm -r -f -skipTrash "$out" >/dev/null
  docker rm -f "$c" >/dev/null 2>&1 || true
  docker compose run -d --name "$c" client hadoop jar /jobs/wiki-hadoop.jar wikihadoop.DomainHourTotals $INPUTS "$out" >/dev/null
  if [ "$kill" != none ]; then
    while :; do
      p=$(map_progress "$c")
      if [ -n "$p" ] && awk -v p="$p" -v k="$KILL_AT" 'BEGIN { exit !(p >= k) }'; then break; fi
      [ "$(docker inspect -f '{{.State.Running}}' "$c")" = true ] || break
      sleep 2
    done
    victim=$(busiest_worker)
    killed_at=$p
    case $kill in
      worker)      docker compose kill -s KILL "$victim" >/dev/null 2>&1 ;;
      datanode)    docker compose exec -T "$victim" bash -c 'kill -9 $(pgrep -f -- -Dproc_datanode)' ;;
      nodemanager) docker compose exec -T "$victim" bash -c 'kill -9 $(pgrep -f -- -Dproc_nodemanager)' ;;
    esac
  fi
  local code
  code=$(docker wait "$c")
  local job
  job=$(docker logs "$c" 2>&1 | grep -oP 'Running job: \Kjob_[0-9_]+' | head -1 || true)
  local counters
  counters=$(docker logs "$c" 2>&1 | grep -oP '^\s+(Launched map tasks|Failed map tasks|Killed map tasks|Data-local map tasks|Rack-local map tasks|Map input records)=\d+' \
             | sed 's/^\s*//' | paste -sd ';' - || true)
  docker rm "$c" >/dev/null
  case $kill in
    worker)      docker compose start "$victim" >/dev/null 2>&1 ;;
    datanode)    docker compose exec -T "$victim" hdfs --daemon start datanode >/dev/null 2>&1 || true ;;
    nodemanager) docker compose exec -T "$victim" yarn --daemon start nodemanager >/dev/null 2>&1 || true ;;
  esac
  local sha=-
  [ "$code" = 0 ] && sha=$(client bash -c "hdfs dfs -cat $out/part-r-00000 | sha256sum" | cut -d' ' -f1)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$kill" "$victim" "$killed_at" "$job" "$code" "$sha" "$counters"
  wait_healthy
}

wait_healthy
results=$(
  run clean none
  run rerun none
  run worker worker
  run datanode datanode
  run nodemanager nodemanager
)
echo "$results"
reference=$(echo "$results" | awk -F'\t' '$1 == "clean" {print $7}')
python - "$reference" <<EOF
import json, sys
reference = sys.argv[1]
runs = []
for line in """$results""".splitlines():
    name, kill, victim, at, job, code, sha, counters = line.split("\t")
    runs.append({"run": name, "killed": kill, "victim": victim, "map_progress_at_kill": at, "job": job,
                 "exit": int(code), "sha256": sha, "identical": sha == reference and code == "0",
                 "counters": dict(c.split("=") for c in counters.split(";") if c)})
print(json.dumps({"input": "$INPUTS", "reference_sha256": reference, "runs": runs,
                  "all_identical": all(r["identical"] for r in runs)}, indent=1))
sys.exit(0 if all(r["identical"] for r in runs) else 1)
EOF
