"""Download one day of Wikimedia pageview data, and Simple English Wikipedia's link tables.

Phase 0 only: the files the reconciliation and the format checks need, nothing more.

One connection at a time (the dumps server caps clients at three per IP and rate limits them),
and a User-Agent that names the client and a contact, as the Wikimedia User-Agent policy asks.
Each file is written to a .part file and renamed when complete; a file already on disk with the
size the server reports is skipped. Every file's size, Last-Modified and sha256 go to
manifest.json, which is what later phases compare against.

Usage: python scripts/phase0/download.py 2026-10-28
The data directory is $WH_DATA_DIR, or ./data.
"""

import hashlib
import json
import os
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

USER_AGENT = "wiki-hadoop-phase0/0.1 (https://eredonda.com) python-urllib"
BASE = "https://dumps.wikimedia.org"
SIMPLEWIKI_TABLES = ("page", "pagelinks", "linktarget", "redirect")


def urls(day: date) -> list[str]:
    ymd = day.strftime("%Y%m%d")
    month = f"{day:%Y}/{day:%Y-%m}"
    out = [f"{BASE}/other/pageview_complete/{month}/pageviews-{ymd}-{agent}.bz2"
           for agent in ("user", "automated")]
    for hour in range(24):
        out.append(f"{BASE}/other/pageviews/{month}/pageviews-{ymd}-{hour:02d}0000.gz")
        out.append(f"{BASE}/other/pageviews/{month}/projectviews-{ymd}-{hour:02d}0000")
    out += [f"{BASE}/simplewiki/latest/simplewiki-latest-{t}.sql.gz" for t in SIMPLEWIKI_TABLES]
    return out


def local_path(root: Path, url: str) -> Path:
    return root / url.removeprefix(BASE + "/")


def head(url: str) -> tuple[int, str]:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as r:
        return int(r.headers["Content-Length"]), r.headers.get("Last-Modified", "")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(url: str, dest: Path, size: int) -> None:
    part = dest.with_name(dest.name + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as r, open(part, "wb") as f:
        for chunk in iter(lambda: r.read(1 << 20), b""):
            f.write(chunk)
    if part.stat().st_size != size:
        raise RuntimeError(f"{url}: got {part.stat().st_size} bytes, server said {size}")
    part.replace(dest)


def main() -> None:
    day = date.fromisoformat(sys.argv[1])
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    manifest_path = root / f"manifest-{day:%Y%m%d}.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for url in urls(day):
        dest = local_path(root, url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        size, modified = head(url)
        rel = dest.relative_to(root).as_posix()
        if dest.exists() and dest.stat().st_size == size and rel in manifest:
            continue
        started = time.monotonic()
        fetch(url, dest, size)
        seconds = time.monotonic() - started
        manifest[rel] = {"url": url, "bytes": size, "last_modified": modified, "sha256": sha256(dest)}
        manifest_path.write_text(json.dumps(manifest, indent=1, sort_keys=True))
        print(f"{rel}  {size / 1e6:,.1f} MB  {seconds:,.0f} s  {size / 1e6 / max(seconds, 1e-3):,.1f} MB/s",
              flush=True)
    print(f"done: {len(manifest)} files, {sum(m['bytes'] for m in manifest.values()) / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
