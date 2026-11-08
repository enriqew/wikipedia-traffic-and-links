"""Check a link graph built by scripts/run-graph.sh, from its log and the outputs it copied out.

1. Every line each job read has a fate, and no dump line was malformed (the jobs refuse to finish
   otherwise; here the log is read back to confirm each job got that far).
2. The pagelinks fates of Edges and Adjacency sum to the pagelinks rows Edges read.
3. With a reference (scripts/reference_graph.py, run for Simple English): the same rows per table,
   the same count for every fate, and the adjacency, titles and redirect titles equal, by the
   sha256 of their canonical form (sorted by page id).

Usage: python scripts/check_graph.py simplewiki 20261101
Exits non-zero if a check fails.
"""

import collections
import hashlib
import json
import os
import re
import sys
from pathlib import Path

FATES = ("EDGE_DIRECT EDGE_VIA DUPLICATE SELF FROM_OTHER_NAMESPACE FROM_REDIRECT FROM_MISSING RED_LINK "
         "TARGET_OTHER_NAMESPACE TARGET_ID_MISSING DOUBLE_REDIRECT REDIRECT_OUT_OF_NS0 REDIRECT_INTERWIKI "
         "REDIRECT_TO_MISSING REDIRECT_WITHOUT_ROW").split()
COUNTER = re.compile(r"^\s+(.+?)=(\d+)$")


def counters(log: Path) -> dict[str, dict[str, int]]:
    """Job name -> counter name -> value, from the counter dump each job prints when it ends."""
    out = collections.defaultdict(dict)
    job = None
    for line in open(log, encoding="utf-8"):
        if line.startswith("=== "):
            job = line[4:].strip()
        m = COUNTER.match(line.rstrip("\n"))
        if job and m:
            out[job][m.group(1)] = int(m.group(2))
    return out


def canonical(path: Path) -> str:
    """sha256 of a page-keyed output, its lines sorted by page id."""
    with open(path, encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f if line.strip()]
    lines.sort(key=lambda s: int(s.split("\t", 1)[0]))
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8") + b"\n")
    return h.hexdigest()


def main() -> int:
    wiki, date = sys.argv[1], sys.argv[2]
    out = Path(os.environ.get("WH_DATA_DIR", "data")) / "graphs" / f"{wiki}-{date}"
    failures = []

    def check(name: str, ok: bool, detail: str) -> None:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}: {detail}")
        if not ok:
            failures.append(name)

    log = (out / "jobs.log").read_text(encoding="utf-8")
    c = counters(out / "jobs.log")
    check("jobs", "=== done" in log and "checks failed" not in log,
          "all four ran and passed their own checks" if "=== done" in log else "the run stopped early")

    rows = {t: c["link-targets"].get(f"{t} ROW", 0) for t in ("page", "linktarget", "redirect")}
    rows["pagelinks"] = c["edges"].get("pagelinks ROW", 0)
    fates = {f: c["edges"].get(f, 0) + c["graph"].get(f, 0) for f in FATES}
    check("pagelinks fates", sum(fates.values()) == rows["pagelinks"],
          f"{sum(fates.values()):,} fates for {rows['pagelinks']:,} rows")
    edges = fates["EDGE_DIRECT"] + fates["EDGE_VIA"]
    print(f"info  {c['graph'].get('ARTICLES', 0):,} articles, {edges:,} edges, "
          f"{c['graph'].get('ARTICLES_WITHOUT_LINKS', 0):,} articles without links")

    ref_path = out / "reference.json"
    if not ref_path.exists():
        print("skip  reference: none for this wiki (scripts/reference_graph.py)")
    else:
        ref = json.loads(ref_path.read_text())
        check("rows", rows == ref["rows"], ", ".join(f"{t} {rows[t]:,}" for t in sorted(rows)))
        differ = {f: (fates[f], ref["fates"].get(f, 0)) for f in FATES if fates[f] != ref["fates"].get(f, 0)}
        check("fates", not differ, "every fate equal" if not differ else f"jobs vs reference: {differ}")
        for name, file in (("adjacency", "graph.adjacency.tsv"), ("titles", "graph.titles.tsv"),
                           ("redirects", "graph.redirects.tsv")):
            digest = canonical(out / file)
            check(name, digest == ref["sha256"][name], f"sha256 {digest[:16]}… "
                  + ("equals the reference" if digest == ref["sha256"][name] else f"!= {ref['sha256'][name][:16]}…"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
