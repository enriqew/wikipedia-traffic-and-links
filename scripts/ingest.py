"""Copy a month of pageview_complete files into HDFS, and prove every byte arrived.

Runs inside the client container (Python 3.10, standard library only), where the downloads are
mounted read-only at /input:

    docker compose run --rm client python3 /scripts/ingest.py 2026-10

Each file listed in the month's download manifest goes to
/raw/pageviews/agent=<user|automated>/date=<YYYY-MM-DD>/<file name>. A file counts as ingested only
when its length in HDFS equals the manifest's byte count and the sha256 of the bytes read back
from HDFS equals the sha256 computed at download. Verified files are recorded in a ledger in HDFS
(/raw/pageviews/_ingest-YYYY-MM.json), so a rerun only handles what is new; the download can still
be running, and a later run picks up the rest. The run ends with fsck over the tree and prints a
report as JSON; it exits non-zero if any file failed verification or HDFS is not healthy.
"""

import json
import re
import subprocess
import sys
import tempfile

ROOT = "/raw/pageviews"
NAME = re.compile(r"pageviews-(\d{4})(\d{2})(\d{2})-(user|automated)\.bz2$")


def run(*args: str, stdin=None) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True, stdin=stdin).stdout


def hdfs_exists(path: str) -> bool:
    return subprocess.run(["hdfs", "dfs", "-test", "-e", path], capture_output=True).returncode == 0


def hdfs_sha256(path: str) -> str:
    cat = subprocess.Popen(["hdfs", "dfs", "-cat", path], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    digest = subprocess.run(["sha256sum"], stdin=cat.stdout, capture_output=True, text=True, check=True)
    if cat.wait() != 0:
        raise RuntimeError(f"hdfs dfs -cat {path} failed")
    return digest.stdout.split()[0]


def main() -> int:
    month = sys.argv[1]
    manifest = json.load(open(f"/input/manifest-{month}.json"))
    ledger_path = f"{ROOT}/_ingest-{month}.json"
    ledger = json.loads(run("hdfs", "dfs", "-cat", ledger_path)) if hdfs_exists(ledger_path) else {}
    failures = []
    for rel, entry in sorted(manifest.items()):
        m = NAME.search(rel)
        if not m:
            continue  # projectviews stay outside HDFS; the reconciliation reads them locally
        y, mo, d, agent = m.groups()
        dest = f"{ROOT}/agent={agent}/date={y}-{mo}-{d}/{rel.rsplit('/', 1)[1]}"
        if ledger.get(dest, {}).get("sha256") == entry["sha256"]:
            continue
        if not hdfs_exists(dest):
            run("hdfs", "dfs", "-mkdir", "-p", dest.rsplit("/", 1)[0])
            run("hdfs", "dfs", "-put", f"/input/{rel}", dest)
        length = int(run("hdfs", "dfs", "-stat", "%b", dest))
        replication = int(run("hdfs", "dfs", "-stat", "%r", dest))
        digest = hdfs_sha256(dest)
        ok = length == entry["bytes"] and digest == entry["sha256"]
        print(f"{'ok' if ok else 'MISMATCH'}  {dest}  {length:,} bytes", flush=True)
        if not ok:
            failures.append(dest)
            continue
        ledger[dest] = {"source": rel, "bytes": length, "sha256": digest, "replication": replication}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(ledger, f, indent=1, sort_keys=True)
        run("hdfs", "dfs", "-put", "-f", f.name, ledger_path)

    fsck = run("hdfs", "fsck", ROOT)
    healthy = "is HEALTHY" in fsck
    def figure(label: str) -> str:
        found = re.search(rf"{label}:\s*([^\n]+)", fsck)
        return found.group(1).strip() if found else ""
    report = {
        "month": month,
        "files": len(ledger),
        "bytes": sum(v["bytes"] for v in ledger.values()),
        "agents": {a: sum(1 for k in ledger if f"agent={a}/" in k) for a in ("user", "automated")},
        "failures": failures,
        "fsck": {"healthy": healthy, "total_blocks": figure("Total blocks \\(validated\\)"),
                 "under_replicated": figure("Under-replicated blocks"),
                 "average_replication": figure("Average block replication")},
    }
    print(json.dumps(report, indent=1))
    return 0 if healthy and not failures else 1


if __name__ == "__main__":
    sys.exit(main())
