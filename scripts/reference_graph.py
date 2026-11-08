"""Build a wiki's link graph on one machine, as the oracle for the MapReduce jobs (plan section 13).

The same rules as LinkTargets, Redirects, Edges and Adjacency, written independently: its own
parser for the dump lines, and dictionaries where the jobs join. Each pagelinks row gets one fate,
checked in the jobs' order (wikihadoop.LinkFate): the namespace it comes from, then where it goes
(a missing link target id, another namespace, a red link, a redirect without a row, to another
wiki, out of namespace 0, to a missing page, or to another redirect), then the page it comes from
(missing, a redirect), then a self-link, then a duplicate, an edge staying direct if any of its
rows is.

Writes $WH_DATA_DIR/graphs/<wiki>-<date>/reference.json: the fate counts, the row counts per
table, and the sha256 of the adjacency, titles and redirect titles in canonical form (sorted by
page id), which scripts/check_graph.py compares with the jobs' output.

Usage: python scripts/reference_graph.py simplewiki 20261101
"""

import collections
import gzip
import hashlib
import json
import os
import re
import sys
from pathlib import Path

STRING = re.compile(r"'((?:[^'\\]|\\.)*)'")
NUMBER = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?")
ESCAPES = {"0": "\0", "n": "\n", "r": "\r", "Z": "\x1a", "\\": "\\", "'": "'", '"': '"'}


def unescape(s: str) -> str:
    out = []
    i = 0
    while i < len(s):
        if s[i] == "\\":
            out.append(ESCAPES[s[i + 1]])  # a KeyError is an escape mysqldump does not write
            i += 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def values(line: str) -> list:
    """The values of one tuple line '(...),' or '(...);'."""
    if not (line.endswith("),") or line.endswith(");")):
        raise ValueError(f"not a tuple line: {line[:100]}")
    body = line[1:-2]
    out = []
    i = 0
    while i <= len(body):
        if i < len(body) and body[i] == "'":
            m = STRING.match(body, i)
            if m is None:
                raise ValueError(f"unterminated string: {line[:100]}")
            out.append(unescape(m.group(1)))
            i = m.end()
        else:
            j = body.find(",", i)
            j = len(body) if j < 0 else j
            token = body[i:j]
            if token != "NULL" and not NUMBER.fullmatch(token):
                raise ValueError(f"not a value: {token!r} in {line[:100]}")
            out.append(None if token == "NULL" else token)
            i = j
        if i < len(body) and body[i] != ",":
            raise ValueError(f"no comma after a value: {line[:100]}")
        i += 1  # the comma
    return out


def rows(path: Path, width: int):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.startswith("("):
                v = values(line.rstrip("\n"))
                if len(v) != width:
                    raise ValueError(f"{path.name}: {len(v)} values, the schema has {width}")
                yield v


def canonical(lines) -> str:
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8") + b"\n")
    return h.hexdigest()


def main() -> None:
    wiki, date = sys.argv[1], sys.argv[2]
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    dump = root / wiki / date
    counts = {}

    pages = {}   # namespace 0 page id -> (title, is_redirect)
    by_title = {}
    n = 0
    for v in rows(dump / f"{wiki}-{date}-page.sql.gz", 12):
        n += 1
        if v[1] == "0":
            pid = int(v[0])
            pages[pid] = (v[2], v[3] == "1")
            if v[2] in by_title:
                raise SystemExit(f"two pages titled {v[2]!r}")
            by_title[v[2]] = pid
    counts["page"] = n

    hop = {}  # redirect page id -> (namespace, title, interwiki)
    for v in rows(dump / f"{wiki}-{date}-redirect.sql.gz", 5):
        hop[int(v[0])] = (v[1], v[2], v[3] or "")
    counts["redirect"] = len(hop)

    def resolve(ns: str, title: str):
        """(article id, 'direct'|'via') or (None, fate) for a link target."""
        if ns != "0":
            return None, "TARGET_OTHER_NAMESPACE"
        pid = by_title.get(title)
        if pid is None:
            return None, "RED_LINK"
        if not pages[pid][1]:
            return pid, "direct"
        h = hop.get(pid)
        if h is None:
            return None, "REDIRECT_WITHOUT_ROW"
        rns, rtitle, interwiki = h
        if interwiki:
            return None, "REDIRECT_INTERWIKI"
        if rns != "0":
            return None, "REDIRECT_OUT_OF_NS0"
        target = by_title.get(rtitle)
        if target is None:
            return None, "REDIRECT_TO_MISSING"
        if pages[target][1]:
            return None, "DOUBLE_REDIRECT"
        return target, "via"

    target = {}
    for v in rows(dump / f"{wiki}-{date}-linktarget.sql.gz", 3):
        target[int(v[0])] = resolve(v[1], v[2])
    counts["linktarget"] = len(target)

    fates = collections.Counter()
    out = collections.defaultdict(dict)  # article -> {target: direct?}
    n = 0
    with gzip.open(dump / f"{wiki}-{date}-pagelinks.sql.gz", "rt", encoding="utf-8") as f:
        for line in f:
            if not line.startswith("("):
                continue
            n += 1
            frm, frm_ns, lt = line[1:line.rindex(")")].split(",")
            if frm_ns != "0":
                fates["FROM_OTHER_NAMESPACE"] += 1
                continue
            r = target.get(int(lt))
            if r is None:
                fates["TARGET_ID_MISSING"] += 1
                continue
            to, how = r
            if to is None:
                fates[how] += 1
                continue
            frm = int(frm)
            page = pages.get(frm)
            if page is None:
                fates["FROM_MISSING"] += 1
            elif page[1]:
                fates["FROM_REDIRECT"] += 1
            elif to == frm:
                fates["SELF"] += 1
            elif to in out[frm]:
                fates["DUPLICATE"] += 1
                out[frm][to] = out[frm][to] or how == "direct"
            else:
                out[frm][to] = how == "direct"
    counts["pagelinks"] = n

    articles = sorted(p for p, (_, redirect) in pages.items() if not redirect)
    edges = 0
    for a in articles:
        for direct in out.get(a, {}).values():
            fates["EDGE_DIRECT" if direct else "EDGE_VIA"] += 1
            edges += 1
    if sum(fates.values()) != counts["pagelinks"]:
        raise SystemExit("the fates do not sum to the pagelinks rows")

    report = {
        "wiki": wiki, "date": date, "rows": counts, "fates": dict(sorted(fates.items())),
        "articles": len(articles), "edges": edges,
        "articles_without_links": sum(1 for a in articles if not out.get(a)),
        "sha256": {
            "adjacency": canonical(f"{a}\t{len(out.get(a, {}))}\t{','.join(map(str, sorted(out.get(a, {}))))}"
                                   for a in articles),
            "titles": canonical(f"{a}\t{pages[a][0]}" for a in articles),
            "redirects": canonical(f"{p}\t{pages[p][0]}" for p in sorted(pages) if pages[p][1]),
        },
    }
    dest = root / "graphs" / f"{wiki}-{date}"
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "reference.json").write_text(json.dumps(report, indent=1))
    print(json.dumps({k: report[k] for k in ("rows", "fates", "articles", "edges", "articles_without_links")}, indent=1))


if __name__ == "__main__":
    main()
