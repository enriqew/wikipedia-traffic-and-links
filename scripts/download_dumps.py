"""Download one wiki's link tables from a dated database dump: page, pagelinks, linktarget, redirect.

Pinned to a dump date, never "latest", which moves. The dump's dumpstatus.json lists each table's
file with its size and sha1; a table whose job is not "done" stops the script, and every file,
downloaded or already on disk, must equal that size and sha1. The sha256 is recorded as well,
because that is what the HDFS ingest checks against. Files go to $WH_DATA_DIR/<wiki>/<date>/,
the manifest to $WH_DATA_DIR/manifest-<wiki>-<date>.json.

One connection, the same User-Agent as the pageview download.

Usage: python scripts/download_dumps.py simplewiki 20261101
"""

import hashlib
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from download import BASE, request  # noqa: E402  same client identity as the pageview download

TABLES = ("page", "pagelinks", "linktarget", "redirect")


def hashes(path: Path) -> tuple[str, str]:
    sha1, sha256 = hashlib.sha1(), hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha1.update(chunk)
            sha256.update(chunk)
    return sha1.hexdigest(), sha256.hexdigest()


def published(wiki: str, dump: str) -> dict[str, dict]:
    """File name -> {size, sha1, url} for the four tables, from the dump's status file."""
    with urllib.request.urlopen(request(f"{BASE}/{wiki}/{dump}/dumpstatus.json"), timeout=60) as r:
        status = json.load(r)
    out = {}
    for table in TABLES:
        job = status["jobs"].get(f"{table}table", {})
        if job.get("status") != "done":
            raise SystemExit(f"{wiki} {dump}: the {table} table is '{job.get('status')}', not done yet")
        for name, f in job["files"].items():
            out[name] = {"size": f["size"], "sha1": f["sha1"], "url": f"{BASE}/{wiki}/{dump}/{name}"}
    return out


def main() -> None:
    wiki, dump = sys.argv[1], sys.argv[2]
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    dest_dir = root / wiki / dump
    dest_dir.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for name, f in published(wiki, dump).items():
        dest = dest_dir / name
        state = "on disk"
        if not (dest.exists() and dest.stat().st_size == f["size"] and hashes(dest)[0] == f["sha1"]):
            started = time.monotonic()
            part = dest.with_name(name + ".part")
            with urllib.request.urlopen(request(f["url"]), timeout=120) as r, open(part, "wb") as out:
                for chunk in iter(lambda: r.read(1 << 20), b""):
                    out.write(chunk)
            part.replace(dest)
            seconds = time.monotonic() - started
            state = f"{f['size'] / 1e6:,.1f} MB in {seconds:,.0f} s"
        sha1, sha256 = hashes(dest)
        if dest.stat().st_size != f["size"] or sha1 != f["sha1"]:
            raise SystemExit(f"{name}: {dest.stat().st_size} bytes, sha1 {sha1}; "
                             f"the dump publishes {f['size']} bytes, sha1 {f['sha1']}")
        manifest[name] = {"url": f["url"], "bytes": f["size"], "sha1": sha1, "sha256": sha256}
        print(f"ok  {name}  {state}", flush=True)
    path = root / f"manifest-{wiki}-{dump}.json"
    path.write_text(json.dumps(manifest, indent=1, sort_keys=True))
    print(f"done: {len(manifest)} files, {sum(m['bytes'] for m in manifest.values()) / 1e6:,.1f} MB, "
          f"each equal to the size and sha1 the dump publishes")


if __name__ == "__main__":
    main()
