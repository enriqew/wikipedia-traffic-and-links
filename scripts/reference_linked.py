"""Set a wiki's views against its PageRank on one machine, as the oracle for the joins (plan section 15).

The same rules as MonthViews, TitleTargets, ViewTargets and LinkedVsRead, written independently:
the titles and redirects come from the page and redirect dumps through reference_graph.py's
parser, not from the cluster's graph, and the views from the raw pageview_complete files of the
window, read here line by line. A viewed title goes to the article it names, or through one
redirect hop to the article the redirect points at; otherwise to a fate: a redirect that reaches no
article, a title in another namespace (for the wikis in wikis.tsv), no page, or the unknown title
'-'. The ranks are the PageRank run's own (scripts/run-pagerank.sh), the input of the join.

Writes $WH_DATA_DIR/linked/<wiki>-<date>/reference.json (the views per fate and agent, the
articles, and the sha256 of the table sorted by page id) and reference.tsv (the table), which
scripts/check_linked.py compares with the cluster's.

Usage: python scripts/reference_linked.py simplewiki 20261101 2026-10-01 31
"""

import collections
import bz2
import hashlib
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reference_graph import rows  # noqa: E402

FATES = ("ARTICLE", "REDIRECT", "REDIRECT_ELSEWHERE", "OTHER_NAMESPACE", "NO_ARTICLE", "UNKNOWN_TITLE")


def read_day(path: str, wiki: str) -> tuple[dict, int]:
    """One pageview_complete file: the wiki's views per title, and how many of its rows were kept."""
    prefix = (wiki + " ").encode()
    views = collections.Counter()
    kept = 0
    with bz2.open(path, "rb") as f:
        for line in f:
            if not line.startswith(prefix):
                continue
            fields = line.rstrip(b"\n").decode("utf-8").split(" ")
            if len(fields) != 6 or not fields[4].isdigit():
                raise ValueError(f"{path}: not a pageview row: {line[:100]!r}")
            views[fields[1]] += int(fields[4])
            kept += 1
    return dict(views), kept


def namespaces(code: str) -> set[str]:
    table = Path(__file__).resolve().parent.parent / "mapreduce/src/main/resources/wikis.tsv"
    for line in table.read_text(encoding="utf-8").splitlines():
        f = line.split("\t")
        if f[0] == code:
            return set(f[2].split("|"))
    return set()


def main() -> None:
    wiki, dump_date, start, days = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
    code = wiki.removesuffix("wiki") + ".wikipedia"
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    dump = root / wiki / dump_date
    dest = root / "linked" / f"{wiki}-{dump_date}"
    dest.mkdir(parents=True, exist_ok=True)

    pages = {}     # namespace 0 page id -> (title, is_redirect)
    by_title = {}  # namespace 0 title -> page id
    for v in rows(dump / f"{wiki}-{dump_date}-page.sql.gz", 12):
        if v[1] == "0":
            pages[int(v[0])] = (v[2], v[3] == "1")
            by_title[v[2]] = int(v[0])
    hop = {}
    for v in rows(dump / f"{wiki}-{dump_date}-redirect.sql.gz", 5):
        hop[int(v[0])] = (v[1], v[2], v[3] or "")

    def target(title: str):
        """(article id, 'article'|'redirect') or (None, None) for a title of namespace 0 that is a
        redirect reaching no article; None when no page of namespace 0 has the title."""
        pid = by_title.get(title)
        if pid is None:
            return None
        if not pages[pid][1]:
            return pid, "article"
        h = hop.get(pid)
        if h is None or h[2] or h[0] != "0":
            return None, None
        to = by_title.get(h[1])
        if to is None or pages[to][1]:
            return None, None
        return to, "redirect"

    ranks = {}
    with open(root / "pagerank" / f"{wiki}-{dump_date}" / "ranks.tsv", encoding="utf-8") as f:
        for line in f:
            page, rank = line.rstrip("\n").split("\t")
            ranks[int(page)] = int(rank)

    first = date.fromisoformat(start)
    files = []
    for i in range(days):
        d = first + timedelta(days=i)
        for agent in ("user", "automated"):
            p = root / "other/pageview_complete" / f"{d:%Y}" / f"{d:%Y-%m}" / f"pageviews-{d:%Y%m%d}-{agent}.bz2"
            if not p.exists():
                raise SystemExit(f"missing {p}")
            files.append((agent, str(p)))
    totals = {"user": collections.Counter(), "automated": collections.Counter()}
    kept = 0
    with ProcessPoolExecutor(max_workers=int(os.environ.get("WH_PROCESSES", "8"))) as pool:
        for (agent, _), (views, n) in zip(files, pool.map(read_day, [p for _, p in files], [code] * len(files))):
            totals[agent].update(views)
            kept += n
    print(f"{len(files)} files, {kept:,} {code} rows", flush=True)

    other = namespaces(code)
    fates = {agent: collections.Counter() for agent in totals}
    titles = collections.Counter()
    user = collections.Counter()
    automated = collections.Counter()
    through = collections.Counter()
    for title in set(totals["user"]) | set(totals["automated"]):
        u, a = totals["user"][title], totals["automated"][title]
        t = None if title == "-" else target(title)
        if title == "-":
            fate = "UNKNOWN_TITLE"
        elif t is None:
            colon = title.find(":")
            fate = "OTHER_NAMESPACE" if colon > 0 and title[:colon].lower() in other else "NO_ARTICLE"
        elif t[0] is None:
            fate = "REDIRECT_ELSEWHERE"
        else:
            fate = "ARTICLE" if t[1] == "article" else "REDIRECT"
            user[t[0]] += u
            automated[t[0]] += a
            if fate == "REDIRECT":
                through[t[0]] += u
        fates["user"][fate] += u
        fates["automated"][fate] += a
        titles[fate] += 1

    articles = sorted(pid for pid, (_, redirect) in pages.items() if not redirect)
    if set(articles) != set(ranks):
        raise SystemExit("the ranks are not one per article of the dump")
    lines = [f"{pid}\t{pages[pid][0]}\t{ranks[pid]}\t{user[pid]}\t{automated[pid]}\t{through[pid]}"
             for pid in articles]
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8") + b"\n")
    (dest / "reference.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    report = {
        "wiki": code, "window": {"start": start, "days": days}, "dump": dump_date,
        "views": {agent: sum(totals[agent].values()) for agent in totals},
        "fates": {agent: {f: fates[agent][f] for f in FATES} for agent in fates},
        "titles": {f: titles[f] for f in FATES},
        "articles": len(articles), "articles_viewed": sum(1 for pid in articles if pid in user),
        "linked_sha256": h.hexdigest(),
    }
    (dest / "reference.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
