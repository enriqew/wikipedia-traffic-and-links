#!/usr/bin/env bash
# Run Maven inside Docker on the mapreduce module, with the same Java (11) as the cluster.
# The local repository lives in a named volume, so dependencies download once.
#
#   scripts/mvn.sh package          # build mapreduce/target/wiki-hadoop.jar, running the tests
#   scripts/mvn.sh test
set -euo pipefail
cd "$(dirname "$0")/.."
src="$(pwd)/mapreduce"
# Git Bash on Windows rewrites /src-like arguments; keep them as written.
export MSYS_NO_PATHCONV=1
if command -v cygpath >/dev/null 2>&1; then
  src="$(cygpath -w "$src")"
fi
exec docker run --rm -v "$src:/src" -v wiki-hadoop-m2:/root/.m2 -w /src \
  maven:3.9.9-eclipse-temurin-11 mvn -B -q "$@"
