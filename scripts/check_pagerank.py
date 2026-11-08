"""Check a PageRank run of scripts/run-pagerank.sh, from the files it copied out.

1. Mass, per iteration from iterations.json: the mass written plus N shares plus the carry equals
   the total, N x 10^9 (the driver refuses to go on otherwise; this reads it back).
2. The ranks: one per article, and with the last carry they sum to the total.
3. With a reference (scripts/reference_pagerank.py, run for Simple English): the top 1,000 the
   same pages in the same order, and every rank within 10^-4 of NetworkX's, relative.

Usage: python scripts/check_pagerank.py simplewiki 20261101
Exits non-zero if a check fails.
"""

import json
import os
import sys
from pathlib import Path

TOP = 1000
TOLERANCE = 1e-4


def main() -> int:
    wiki, date = sys.argv[1], sys.argv[2]
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    out = root / "pagerank" / f"{wiki}-{date}"
    failures = []

    def check(name: str, ok: bool, detail: str) -> None:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}: {detail}")
        if not ok:
            failures.append(name)

    log = json.loads((out / "iterations.json").read_text())
    n, mass, its = log["articles"], log["mass"], log["iterations"]
    bad = [i["iteration"] for i in its if i["link_mass"] + n * i["share"] + i["carry"] != mass]
    check("mass", not bad and mass == n * log["units_per_article"],
          f"{mass:,} units after each of {len(its)} iterations" if not bad else f"iterations {bad} lose mass")
    changes = [i["change_of_previous"] for i in its if i["change_of_previous"] is not None]
    seconds = sum(i["seconds"] for i in its)
    print(f"info  {len(its)} iterations, {seconds / 60:.1f} min; last change {changes[-1]:.2e} of the mass; "
          f"{its[-1]['shuffle_bytes'] / 1e9:.2f} GB shuffled and {its[-1]['bytes_read'] / 1e9:.2f} GB read in the last")

    ranks = {}
    with open(out / "ranks.tsv", encoding="utf-8") as f:
        for line in f:
            page, rank = line.rstrip("\n").split("\t")
            ranks[int(page)] = int(rank)
    total = sum(ranks.values()) + its[-1]["carry"]
    check("ranks", len(ranks) == n and total == mass, f"{len(ranks):,} articles, ranks plus carry {total:,}")

    ref_path = out / "reference.tsv"
    if not ref_path.exists():
        print("skip  reference: none for this wiki (scripts/reference_pagerank.py)")
        return 1 if failures else 0
    ref = {}
    with open(ref_path, encoding="utf-8") as f:
        for line in f:
            page, value = line.rstrip("\n").split("\t")
            ref[int(page)] = float(value)
    check("same articles", set(ref) == set(ranks), f"{len(ref):,} in the reference")
    ours = sorted(ranks, key=lambda p: (-ranks[p], p))[:TOP]
    theirs = sorted(ref, key=lambda p: (-ref[p], p))[:TOP]
    first = next((i for i in range(TOP) if ours[i] != theirs[i]), None)
    check(f"top {TOP:,}", first is None, "same pages in the same order" if first is None
          else f"first difference at position {first + 1}")
    worst = max(abs(ranks[p] / mass - ref[p]) / ref[p] for p in ref)
    check("values", worst < TOLERANCE, f"largest relative difference {worst:.2e} (tolerance {TOLERANCE:g})")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
