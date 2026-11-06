"""Download a month of Wikimedia pageview data: pageview_complete (user and automated) and projectviews.

For each day of the month, the day's two pageview_complete files; and the month's hourly
projectviews files, which the reconciliation compares the jobs' user totals with. An hourly file
is stamped with the end of the hour it covers (docs/PHASE0.md), so the month's hours are the files
stamped from 01:00 on its first day to 00:00 on the first day of the next month.

At most --connections downloads at once (the dumps server caps clients at three per IP), with a
User-Agent that names the client and a contact, as the Wikimedia User-Agent policy asks. Each file
goes to a .part file, is checked against the size the server reported, and is renamed when
complete. A file already on disk with that size is not downloaded again. Every file's size,
Last-Modified and sha256 go to manifest-YYYY-MM.json, which the HDFS ingest checks against.

Usage: python scripts/download.py 2026-10 [--connections 2]
The data directory is $WH_DATA_DIR, or ./data.
"""

import argparse
import hashlib
import json
import os
import queue
import threading
import time
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

USER_AGENT = "wiki-hadoop/0.1 (https://eredonda.com) python-urllib"
BASE = "https://dumps.wikimedia.org"


def month_days(month: str) -> list[date]:
    first = datetime.strptime(month, "%Y-%m").date()
    nxt = (first.replace(day=28) + timedelta(days=4)).replace(day=1)
    return [first + timedelta(days=i) for i in range((nxt - first).days)]


def urls(month: str) -> list[str]:
    days = month_days(month)
    out = []
    for day in days:
        for agent in ("automated", "user"):  # largest first, so the queue ends on small files
            out.append(f"{BASE}/other/pageview_complete/{day:%Y}/{day:%Y-%m}/"
                       f"pageviews-{day:%Y%m%d}-{agent}.bz2")
    start = datetime.combine(days[0], datetime.min.time()) + timedelta(hours=1)
    for h in range(len(days) * 24):
        t = start + timedelta(hours=h)
        out.append(f"{BASE}/other/pageviews/{t:%Y}/{t:%Y-%m}/projectviews-{t:%Y%m%d-%H}0000")
    return out


def request(url: str, method: str = "GET") -> urllib.request.Request:
    return urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT})


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class Downloader:
    def __init__(self, root: Path, month: str):
        self.root = root
        self.manifest_path = root / f"manifest-{month}.json"
        self.manifest = json.loads(self.manifest_path.read_text()) if self.manifest_path.exists() else {}
        self.lock = threading.Lock()

    def record(self, rel: str, entry: dict) -> None:
        with self.lock:
            self.manifest[rel] = entry
            tmp = self.manifest_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.manifest, indent=1, sort_keys=True))
            tmp.replace(self.manifest_path)

    def one(self, url: str) -> str:
        dest = self.root / url.removeprefix(BASE + "/")
        rel = dest.relative_to(self.root).as_posix()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(request(url, "HEAD"), timeout=60) as r:
            size = int(r.headers["Content-Length"])
            modified = r.headers.get("Last-Modified", "")
        if dest.exists() and dest.stat().st_size == size:
            if rel not in self.manifest:
                self.record(rel, {"url": url, "bytes": size, "last_modified": modified, "sha256": sha256(dest)})
            return f"{rel}  on disk"
        started = time.monotonic()
        part = dest.with_name(dest.name + ".part")
        with urllib.request.urlopen(request(url), timeout=120) as r, open(part, "wb") as f:
            for chunk in iter(lambda: r.read(1 << 20), b""):
                f.write(chunk)
        if part.stat().st_size != size:
            raise RuntimeError(f"{url}: got {part.stat().st_size} bytes, server said {size}")
        part.replace(dest)
        self.record(rel, {"url": url, "bytes": size, "last_modified": modified, "sha256": sha256(dest)})
        seconds = time.monotonic() - started
        return f"{rel}  {size / 1e6:,.1f} MB  {seconds:,.0f} s  {size / 1e6 / max(seconds, 1e-3):,.1f} MB/s"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("month")
    p.add_argument("--connections", type=int, default=2)
    args = p.parse_args()
    if not 1 <= args.connections <= 3:
        raise SystemExit("the dumps server allows at most three connections per IP")
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    root.mkdir(parents=True, exist_ok=True)
    d = Downloader(root, args.month)
    todo: queue.Queue = queue.Queue()
    for url in urls(args.month):
        todo.put(url)
    failures: list[str] = []

    def worker() -> None:
        while True:
            try:
                url = todo.get_nowait()
            except queue.Empty:
                return
            for attempt in range(3):
                try:
                    print(d.one(url), flush=True)
                    break
                except Exception as e:  # noqa: BLE001 - retried, then reported
                    if attempt == 2:
                        failures.append(f"{url}: {e}")
                        print(f"FAILED {url}: {e}", flush=True)
                    else:
                        time.sleep(10 * (attempt + 1))

    threads = [threading.Thread(target=worker) for _ in range(args.connections)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total = sum(m["bytes"] for m in d.manifest.values())
    print(f"done: {len(d.manifest)} files, {total / 1e9:.2f} GB, {len(failures)} failed")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
