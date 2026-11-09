"""Check a linked-against-read run of scripts/run-linked.sh, and say what it shows (plan section 15).

1. MonthViews: each wiki's user and automated views over the window equal DomainHourTotals' over
   the wiki's desktop and mobile domain codes and the same days (run-window.sh's output, phase 3).
2. ViewTargets: the views of every fate sum to the wiki's views, per agent.
3. LinkedVsRead: every article of the graph once, its rank the PageRank run's, the ranks' sum the
   run's, and the articles' views the views of the first two fates, the article's own title and the
   redirects to it; the same from the table it wrote and from its counters.
4. With a reference (scripts/reference_linked.py, run for Simple English): the same views per fate
   and agent, and the same table by the sha256 of its lines sorted by page id.

Then the comparison: Spearman's correlation of PageRank and user views over all articles (tied
values share their average rank), positions by each (ties broken by page id), the overlap of each top 100 and top 1,000 with the other's, and the
outliers both ways, written to linked-summary.json beside the table.

Usage: python scripts/check_linked.py enwiki 20261101 oct
Exits non-zero if a check fails.
"""

import collections
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

FATES = ("ARTICLE", "REDIRECT", "REDIRECT_ELSEWHERE", "OTHER_NAMESPACE", "NO_ARTICLE", "UNKNOWN_TITLE")
GROUP = re.compile(r"^\t([^\t=]+)$")
COUNTER = re.compile(r"^\t\t(.+?)=(\d+)$")
TOP = 1000
OUTLIERS = 50


def counters(log: Path) -> dict[str, dict[tuple[str, str], int]]:
    """Job name -> (counter group, counter name) -> value, from the dump each job prints at its end."""
    out = collections.defaultdict(dict)
    job = group = None
    for line in open(log, encoding="utf-8"):
        line = line.rstrip("\n")
        if line.startswith("=== "):
            job = line[4:].strip()
        elif m := GROUP.match(line):
            group = m.group(1)
        elif (m := COUNTER.match(line)) and job and group:
            out[job][(group, m.group(1))] = int(m.group(2))
    return out


def domain_totals(path: Path, codes: tuple[str, str]) -> int:
    """A wiki's views in a DomainHourTotals output (date domain hour views), over its two codes."""
    total = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            _, domain, _, views = line.rstrip("\n").split("\t")
            if domain in codes:
                total += int(views)
    return total


def main() -> int:
    wiki, dump_date, run = sys.argv[1], sys.argv[2], sys.argv[3]
    lang = wiki.removesuffix("wiki")
    code = lang + ".wikipedia"
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    out = root / "linked" / f"{wiki}-{dump_date}"
    failures = []

    def check(name: str, ok: bool, detail: str) -> None:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}: {detail}")
        if not ok:
            failures.append(name)

    mv = counters(root / "runs" / run / "month-views.log")["month-views"]
    month = {agent: mv.get(("month views", f"{code} {agent}"), 0) for agent in ("user", "automated")}
    for agent in ("user", "automated"):
        hourly = domain_totals(root / "runs" / run / f"domain-hour-{agent}.part.tsv", (lang, lang + ".m"))
        check(f"month views, {agent}", month[agent] == hourly,
              f"{month[agent]:,} in MonthViews, {hourly:,} in DomainHourTotals over {lang} and {lang}.m")

    c = counters(out / "jobs.log")
    vt = c["view-targets"]
    fates = {agent: {f: vt.get((group, f), 0) for f in FATES}
             for agent, group in (("user", "user views"), ("automated", "automated views"))}
    for agent in ("user", "automated"):
        s = sum(fates[agent].values())
        check(f"fates, {agent}", s == month[agent], f"{s:,} over the six fates")
    total = month["user"]
    for f in FATES:
        n = fates["user"][f]
        print(f"      {f:20s} {n:>15,} {n / total if total else 0:8.3%}  automated {fates['automated'][f]:,}")

    ranks = {}
    with open(root / "pagerank" / f"{wiki}-{dump_date}" / "ranks.tsv", encoding="utf-8") as f:
        for line in f:
            page, rank = line.rstrip("\n").split("\t")
            ranks[int(page)] = int(rank)
    pages, titles, rank, user, automated, through = [], [], [], [], [], []
    with open(out / "linked.tsv", encoding="utf-8") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            pages.append(int(p[0]))
            titles.append(p[1])
            rank.append(int(p[2]))
            user.append(int(p[3]))
            automated.append(int(p[4]))
            through.append(int(p[5]))
    lc = c["linked"]
    k = "wikihadoop.LinkedVsRead$Counts"
    same_ranks = len(pages) == len(set(pages)) == len(ranks) and all(ranks.get(p) == r for p, r in zip(pages, rank))
    check("articles", same_ranks and lc.get((k, "ARTICLES")) == len(pages),
          f"{len(pages):,} lines, each article once with the rank PageRank wrote")
    check("rank sum", sum(rank) == sum(ranks.values()) == lc.get((k, "RANK_SUM")), f"{sum(rank):,} units")
    direct = fates["user"]["ARTICLE"] + fates["user"]["REDIRECT"]
    check("article views", sum(user) == direct == lc.get((k, "USER_VIEWS"))
          and sum(through) == fates["user"]["REDIRECT"] == lc.get((k, "USER_THROUGH_REDIRECTS"))
          and sum(automated) == fates["automated"]["ARTICLE"] + fates["automated"]["REDIRECT"],
          f"{sum(user):,} user views reach an article, {sum(through):,} of them through a redirect")

    lines = sorted(zip(pages, titles, rank, user, automated, through))
    h = hashlib.sha256()
    for row in lines:
        h.update(("\t".join(str(x) for x in row) + "\n").encode("utf-8"))
    digest = h.hexdigest()
    ref_path = out / "reference.json"
    if ref_path.exists():
        ref = json.loads(ref_path.read_text())
        check("reference fates", ref["fates"] == fates, "the same views per fate and agent")
        check("reference table", ref["linked_sha256"] == digest, f"sha256 {digest[:16]}...")
    else:
        print("skip  reference: none for this wiki (scripts/reference_linked.py)")

    n = len(lines)
    pid = np.array([r[0] for r in lines])
    rk = np.array([r[2] for r in lines], dtype=np.int64)
    vw = np.array([r[3] for r in lines], dtype=np.int64)
    by_rank = np.lexsort((pid, -rk))  # positions: larger first, ties by page id
    by_views = np.lexsort((pid, -vw))
    pos_rank = np.empty(n, dtype=np.int64)
    pos_rank[by_rank] = np.arange(1, n + 1)
    pos_views = np.empty(n, dtype=np.int64)
    pos_views[by_views] = np.arange(1, n + 1)
    spearman = float(spearmanr(rk, vw).statistic)
    main_page = {i for i, r in enumerate(lines) if r[1] == "Main_Page"}
    mass = int(rk.sum())
    total_views = int(vw.sum())

    def row(i: int) -> dict:
        return {"page_id": int(pid[i]), "title": lines[i][1], "rank_share": int(rk[i]) / mass,
                "user_views": int(vw[i]), "automated_views": lines[i][4],
                "user_through_redirects": lines[i][5],
                "pagerank_position": int(pos_rank[i]), "views_position": int(pos_views[i])}

    top_views = [i for i in by_views[:TOP + 1] if i not in main_page][:TOP]
    top_rank = [i for i in by_rank[:TOP + 1] if i not in main_page][:TOP]
    read_beyond = sorted(top_views, key=lambda i: (-pos_rank[i], pid[i]))[:OUTLIERS]
    linked_beyond = sorted(top_rank, key=lambda i: (vw[i], pid[i]))[:OUTLIERS]
    overlap = {f"top_{t}": len(set(by_rank[:t]) & set(by_views[:t])) for t in (100, 1000)}
    summary = {
        "wiki": code, "dump": dump_date, "views_run": run, "articles": n,
        "articles_viewed": int(np.count_nonzero(vw)), "user_views": total_views,
        "spearman": spearman, "overlap": overlap,
        "top_by_pagerank": [row(i) for i in by_rank[:20]],
        "top_by_views": [row(i) for i in by_views[:20]],
        "read_beyond_links": [row(i) for i in read_beyond],
        "linked_beyond_reads": [row(i) for i in linked_beyond],
        "fates": fates, "linked_sha256": digest,
    }
    (out / "linked-summary.json").write_text(json.dumps(summary, indent=1, ensure_ascii=False), encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    print(f"info  {n:,} articles, {summary['articles_viewed']:,} with a user view; Spearman {spearman:.3f}; "
          f"top 100 shared {overlap['top_100']}, top 1,000 shared {overlap['top_1000']}")
    for name, key in (("read beyond its links", "read_beyond_links"), ("linked beyond its reads", "linked_beyond_reads")):
        print(f"info  {name}:")
        for r in summary[key][:15]:
            print(f"        {r['title'][:50]:50s} views #{r['views_position']:<9,} pagerank #{r['pagerank_position']:,}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
