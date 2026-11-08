"""Copy one wiki's link-table dumps into HDFS in chunks a map can each take, and prove they arrived.

Runs inside the client container (Python 3.10, standard library only), where the downloads are
mounted read-only at /input:

    docker compose run --rm -T client python3 /scripts/ingest_dumps.py simplewiki 20261101

gzip does not split, so a 7 GB dump would be a single map. Each table is decompressed as a stream
and cut at line ends into chunks of about CHUNK uncompressed bytes, each written as gzip to
/raw/dumps/<wiki>/<date>/<table>/<wiki>-<date>-<table>-NNNNN.sql.gz. A line is one row
(docs/PHASE0.md), so no row is split. A table counts as ingested when:

- the dump file's sha1, read while it is decompressed, equals the download manifest's, which is
  the one the dump publishes (scripts/download_dumps.py);
- the chunks read back from HDFS have, each, the sha256 of the bytes written; and, decompressed
  and concatenated in order, the sha256 of the decompressed dump.

Verified tables go to a ledger in HDFS (/raw/dumps/_ingest-<wiki>-<date>.json), so a rerun only
handles what is missing. The run ends with fsck and prints a report as JSON; it exits non-zero if
a table failed or HDFS is not healthy.
"""

import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile

ROOT = os.environ.get("WH_DUMP_ROOT", "/raw/dumps")
CHUNK = int(os.environ.get("WH_DUMP_CHUNK", 1 << 30))  # uncompressed bytes per chunk, cut at a line end
LEVEL = 1        # 154 MB/s against 41 at level 6 in this container, for files 20% larger
BLOCK = 8 << 20


def run(*args: str) -> str:
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def hdfs_exists(path: str) -> bool:
    return subprocess.run(["hdfs", "dfs", "-test", "-e", path], capture_output=True).returncode == 0


class Hashing:
    """A readable stream that hashes what is read from it."""

    def __init__(self, stream, algorithm: str):
        self.stream = stream
        self.hash = hashlib.new(algorithm)

    def read(self, n: int = -1) -> bytes:
        b = self.stream.read(n)
        self.hash.update(b)
        return b


def chunks_of(path: str, table_dir: str, prefix: str):
    """Writes the chunks to HDFS; returns (source sha1, content sha256, lines, [chunk entries])."""
    source = Hashing(open(path, "rb"), "sha1")
    content = hashlib.sha256()
    lines = 0
    entries = []
    with gzip.GzipFile(fileobj=source) as dump:
        pending = b""
        done = False
        while not done:
            tmp = tempfile.NamedTemporaryFile(suffix=".sql.gz", delete=False)
            written = 0
            with gzip.GzipFile(fileobj=tmp, mode="wb", compresslevel=LEVEL, mtime=0) as out:
                while True:
                    block = pending or dump.read(BLOCK)
                    pending = b""
                    if not block:
                        done = True
                        break
                    if written + len(block) >= CHUNK:
                        cut = block.rfind(b"\n", 0, max(CHUNK - written, 0) + 1)
                        if cut < 0:
                            cut = block.find(b"\n", max(CHUNK - written, 0))
                        if cut >= 0:
                            block, pending = block[: cut + 1], block[cut + 1:]
                    out.write(block)
                    content.update(block)
                    lines += block.count(b"\n")
                    written += len(block)
                    if pending:
                        break
            tmp.close()
            if written == 0:
                os.unlink(tmp.name)
                break
            name = f"{prefix}-{len(entries):05d}.sql.gz"
            h = hashlib.sha256()
            with open(tmp.name, "rb") as f:
                for b in iter(lambda: f.read(BLOCK), b""):
                    h.update(b)
            digest = h.hexdigest()
            run("hdfs", "dfs", "-put", tmp.name, f"{table_dir}/{name}")
            entries.append({"path": f"{table_dir}/{name}", "bytes": os.path.getsize(tmp.name),
                            "sha256": digest, "uncompressed": written})
            os.unlink(tmp.name)
        source.read()  # anything after the gzip stream still belongs to the file's sha1
    return source.hash.hexdigest(), content.hexdigest(), lines, entries


def read_back(entries: list[dict]) -> tuple[list[str], str]:
    """Each chunk's sha256 as HDFS returns it, and the sha256 of all of them decompressed in order."""
    content = hashlib.sha256()
    digests = []
    for e in entries:
        cat = subprocess.Popen(["hdfs", "dfs", "-cat", e["path"]], stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL)
        raw = Hashing(cat.stdout, "sha256")
        with gzip.GzipFile(fileobj=raw) as g:
            for block in iter(lambda: g.read(BLOCK), b""):
                content.update(block)
        raw.read()
        if cat.wait() != 0:
            raise RuntimeError(f"hdfs dfs -cat {e['path']} failed")
        digests.append(raw.hash.hexdigest())
    return digests, content.hexdigest()


def main() -> int:
    wiki, date = sys.argv[1], sys.argv[2]
    manifest = json.load(open(f"/input/manifest-{wiki}-{date}.json"))
    ledger_path = f"{ROOT}/_ingest-{wiki}-{date}.json"
    ledger = json.loads(run("hdfs", "dfs", "-cat", ledger_path)) if hdfs_exists(ledger_path) else {}
    failures = []
    for name, entry in sorted(manifest.items()):
        table = re.fullmatch(rf"{wiki}-{date}-([a-z]+)\.sql\.gz", name).group(1)
        if ledger.get(table, {}).get("sha1") == entry["sha1"]:
            continue
        table_dir = f"{ROOT}/{wiki}/{date}/{table}"
        subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", table_dir], capture_output=True)
        run("hdfs", "dfs", "-mkdir", "-p", table_dir)
        sha1, content, lines, entries = chunks_of(f"/input/{wiki}/{date}/{name}", table_dir,
                                                  f"{wiki}-{date}-{table}")
        back, back_content = read_back(entries)
        ok = (sha1 == entry["sha1"] and back_content == content
              and back == [e["sha256"] for e in entries])
        print(f"{'ok' if ok else 'MISMATCH'}  {table}  {len(entries)} chunks  {lines:,} lines  "
              f"{sum(e['uncompressed'] for e in entries):,} bytes uncompressed", flush=True)
        if not ok:
            failures.append(table)
            continue
        ledger[table] = {"source": name, "sha1": sha1, "content_sha256": content, "lines": lines,
                         "chunks": entries}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(ledger, f, indent=1, sort_keys=True)
        run("hdfs", "dfs", "-put", "-f", f.name, ledger_path)

    fsck = run("hdfs", "fsck", f"{ROOT}/{wiki}/{date}")
    healthy = "is HEALTHY" in fsck
    blocks = re.search(r"Total blocks \(validated\):\s*([^\n]+)", fsck)
    report = {
        "wiki": wiki, "date": date, "failures": failures,
        "tables": {t: {"lines": v["lines"], "chunks": len(v["chunks"]),
                       "bytes": sum(c["bytes"] for c in v["chunks"])} for t, v in sorted(ledger.items())},
        "fsck": {"healthy": healthy, "total_blocks": blocks.group(1).strip() if blocks else ""},
    }
    print(json.dumps(report, indent=1))
    return 0 if healthy and not failures else 1


if __name__ == "__main__":
    sys.exit(main())
