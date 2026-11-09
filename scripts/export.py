"""Write the page's data, once every check of the runs behind it holds (plan sections 5 and 16).

In order, stopping at the first failure without writing anything:

1. The checks of each run: scripts/check_window.py over the window (the reconciliation with
   projectviews, pass one's totals, the spikes' hours), and check_graph.py, check_pagerank.py and
   check_linked.py for each wiki of the dump, against their one-machine references where there are
   some (Simple English).
2. scripts/run-hive.sh, then the Hive checks on its results: MonthViews against DomainHourTotals
   and the job's counters; SpikeCandidates' totals against DomainHourTotals per day, wiki and agent;
   LinkedVsRead against the graph's titles and PageRank's ranks page by page, its sums against
   PageRank's mass and ViewTargets' counters, its top 100 by each measure against check_linked.py's;
   the spikes against SpikeHours' and TopSpikes' counters.
3. The runs repeated on purpose, compared now by sha256: DomainHourTotals five times under failure
   (docs/RECOVERY.md), Simple English's graph built twice, its PageRank run twice.

Then writes manifest.json, reading.json, bots.json, linked.json and checks.json to --out (default
export/), as docs/EXPORT.md describes. Every figure in them comes from a job output, a job's
counters, a Hive result or a check's own line.

Usage: uv run python scripts/export.py oct 2026-10-01 31 20261101 [--out export]
Needs WH_DATA_DIR and the cluster up.
"""

import argparse
import collections
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_linked import FATES as VIEW_FATES, counters  # noqa: E402
from check_graph import FATES as LINK_FATES  # noqa: E402

SCHEMA_VERSION = 1
REPO = Path(__file__).resolve().parent.parent
LINKED_WIKIS = ("enwiki", "simplewiki")
AGENTS = ("user", "automated")
CREDITS = [
    "Pageview data: Wikimedia Foundation, pageview_complete and projectviews (dumps.wikimedia.org/other/), CC0.",
    "Link graph: built from the English and Simple English Wikipedia database dumps (page, pagelinks, "
    "linktarget, redirect) of the date named, by Wikipedia's contributors; dumps.wikimedia.org.",
]
# The rules the jobs applied, with the constants export checks against the source the window ran.
RULES = {
    "spike": {"factor": ("SpikeRule", "FACTOR", 10.0), "floor_share": ("SpikeRule", "FLOOR_SHARE", 1e-4)},
    "looks_automated": {"flat_fraction": ("LooksAutomated", "FLAT_FRACTION", 0.5),
                        "one_access": ("LooksAutomated", "ONE_ACCESS", 0.95)},
    "burst": {"share": ("Burst", "SHARE", 0.9)},
}
SPIKE_RULE = ("A day is a spike for an article when its user views are at least factor times its baseline "
              "(a baseline under one counts as one) and at least floor_share of its wiki's user views that day. "
              "The baseline is the median of the article's user views over the window's other days, leaving out "
              "the day before and the day after; days without a row count as zero. Spikes rank by excess, views "
              "minus baseline.")
AUTOMATED_RULE = ("This project's rule, not Wikimedia's classification. Flat: the spike's 24 hourly user views "
                  "vary (standard deviation over mean) less than flat_fraction as much as its wiki's that day. "
                  "Access: at least one_access of its user views came through one access method that carries "
                  "under half of the wiki's user views.")
# The columns of a SpikeHours line: s string, i integer, f float.
SPIKE = "sssiffiiiifsffsfss"
BURST_RULE ="At least share of the day's user views in its busiest hour. Shown beside a spike, not part of the rule."


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def compose(*args: str) -> str:
    """A command in a cluster container, its standard output."""
    return subprocess.run(["docker", "compose", *args], cwd=REPO, check=True, capture_output=True,
                          text=True, encoding="utf-8").stdout


def hdfs_cat(path: str) -> str:
    return compose("exec", "-T", "namenode", "hdfs", "dfs", "-cat", path)


def log_head(log: Path) -> dict:
    """The commit and jar sha256 a run's log opens with, and the jobs it ran."""
    commit = jar = None
    jobs = []
    for line in open(log, encoding="utf-8"):
        if line.startswith("commit "):
            commit = line[7:].strip()
        elif line.startswith("jar sha256 "):
            jar = line[11:].strip()
        elif line.startswith("=== ") and line.strip() != "=== done":
            jobs.append(line[4:].strip())
    return {"commit": commit, "jar_sha256": jar, "jobs": jobs}


def source_at(commit: str, cls: str) -> str:
    sha = commit.split()[0]
    return subprocess.run(["git", "show", f"{sha}:mapreduce/src/main/java/wikihadoop/{cls}.java"], cwd=REPO,
                          check=True, capture_output=True, text=True, encoding="utf-8").stdout


def typed(x: str, t: str):
    """A Hive result field as its column's type (s string, i integer, f float); \\N and non-finite as None."""
    if t == "s":
        return x
    if x == "\\N":
        return None
    if t == "i":
        return int(x)
    v = float(x)
    return v if math.isfinite(v) else None


class Checks:
    def __init__(self):
        self.lines = []

    def run(self, script: str, *args: str) -> None:
        cmd = [sys.executable, str(REPO / "scripts" / script), *args]
        print(f"--- {script} {' '.join(args)}", flush=True)
        env = {**os.environ, "PYTHONUTF8": "1"}  # a pipe would otherwise take the console's code page
        p = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True, encoding="utf-8", env=env)
        sys.stdout.write(p.stdout)
        for line in p.stdout.splitlines():
            m = re.match(r"^(ok  |FAIL|skip|info)  (.*)$", line)
            if not m:
                continue
            status, rest = m.group(1).strip(), m.group(2)
            name, _, detail = rest.partition(": ") if status != "info" else ("", "", rest)
            self.lines.append({"script": f"{script} {' '.join(args)}", "status": status, "name": name,
                               "detail": detail})
        if p.returncode != 0:
            sys.stderr.write(p.stderr)
            raise SystemExit(f"{script} {' '.join(args)} failed: nothing written")

    def check(self, name: str, ok: bool, detail: str) -> None:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}: {detail}", flush=True)
        self.lines.append({"script": "export.py", "status": "ok" if ok else "FAIL", "name": name, "detail": detail})
        if not ok:
            raise SystemExit(f"check failed: {name}: nothing written")


def fmt(x) -> str:
    return f"{x:,}" if isinstance(x, int) else str(x)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("run")
    ap.add_argument("start")
    ap.add_argument("days", type=int)
    ap.add_argument("dump")
    ap.add_argument("--out", default=str(REPO / "export"))
    a = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    data = Path(os.environ["WH_DATA_DIR"])
    start = date.fromisoformat(a.start)
    days = [(start + timedelta(days=i)).isoformat() for i in range(a.days)]
    window = {"start": days[0], "end": days[-1], "days": a.days}
    c = Checks()

    # 1. The runs' own checks.
    c.run("check_window.py", a.run, a.start, str(a.days))
    for wiki in LINKED_WIKIS:
        c.run("check_graph.py", wiki, a.dump)
        c.run("check_pagerank.py", wiki, a.dump)
        c.run("check_linked.py", wiki, a.dump, a.run)

    # 2. Hive, and its checks.
    print("--- run-hive.sh", flush=True)
    bash = os.environ.get("WH_BASH") or shutil.which("bash")
    subprocess.run([bash, str(REPO / "scripts" / "run-hive.sh"), a.run, a.dump], cwd=REPO, check=True)
    hive_dir = data / "hive" / f"{a.run}-{a.dump}"

    def hive(name: str, types: str) -> list[list]:
        rows = []
        with open(hive_dir / f"{name}.tsv", encoding="utf-8") as f:
            for line in f:
                fields = line.rstrip("\n").split("\t")
                if len(fields) != len(types):
                    raise SystemExit(f"{name}: {len(fields)} fields where {len(types)} were expected: nothing written")
                rows.append([typed(x, t) for x, t in zip(fields, types)])
        return rows

    logs = {
        "window": data / "runs" / a.run / "jobs.log",
        "month-views": data / "runs" / a.run / "month-views.log",
        **{f"graph {w}": data / "graphs" / f"{w}-{a.dump}" / "jobs.log" for w in LINKED_WIKIS},
        **{f"pagerank {w}": data / "pagerank" / f"{w}-{a.dump}" / "jobs.log" for w in LINKED_WIKIS},
        **{f"linked {w}": data / "linked" / f"{w}-{a.dump}" / "jobs.log" for w in LINKED_WIKIS},
    }
    runs = {name: {"log": str(p.relative_to(data)).replace("\\", "/"), **log_head(p)} for name, p in logs.items()}
    hive_log = hive_dir / "hive.log"
    head = {}
    for line in open(hive_log, encoding="utf-8", errors="replace"):
        for prefix in ("commit ", "tables sha256 ", "results sha256 "):
            if line.startswith(prefix):
                head[prefix.strip().replace(" ", "_")] = line[len(prefix):].strip()
    runs["hive"] = {"log": str(hive_log.relative_to(data)).replace("\\", "/"),
                    "commit": head.get("commit"), "tables_sha256": head.get("tables_sha256"),
                    "results_sha256": head.get("results_sha256"), "jobs": ["tables", "results"]}
    for name, r in runs.items():
        c.check(f"run {name}", bool(r["commit"]) and "uncommitted" not in r["commit"],
                f"commit {r['commit']}")

    mv_counters = counters(logs["month-views"])["month-views"]
    month = {r[0]: {"titles": r[1], "user": r[2], "automated": r[3]} for r in hive("month-views-totals", "siii")}
    wiki_day = {(r[0], r[1], r[2]): r[3] for r in hive("wiki-day", "sssi")}
    for wiki in sorted(month):
        for agent in AGENTS:
            dh = sum(v for (w, ag, _), v in wiki_day.items() if w == wiki and ag == agent)
            job = mv_counters.get(("month views", f"{wiki} {agent}"))
            c.check(f"hive month views, {wiki} {agent}", month[wiki][agent] == dh == job,
                    f"{fmt(month[wiki][agent])} over MonthViews, {fmt(dh)} over DomainHourTotals, "
                    f"{fmt(job)} in the job's counters")
    titles = mv_counters.get(("wikihadoop.MonthViews$Counts", "TITLES"))
    c.check("hive month titles", sum(m["titles"] for m in month.values()) == titles,
            f"{fmt(titles)} titles, as the job counted")

    access = {(r[0], r[1], r[2]): r[3] for r in hive("access-totals-day", "sssi")}
    wikis = sorted({w for w, _, _ in wiki_day})
    c.check("hive pass-one totals", access == wiki_day and len(access) == len(wikis) * len(AGENTS) * len(days),
            f"{len(access):,} days of {len(wikis)} wikis and two agents, each equal to DomainHourTotals over "
            f"the wiki's two domain codes")

    join = {r[0]: r[1:] for r in hive("linked-join", "sii")}
    top_rows = hive("linked-top", "ssiisiiii")  # measure, wiki, position, page id, title, rank, views...
    totals = {r[0]: r[1:] for r in hive("linked-totals", "siiiiiii")}
    rank_totals = {r[0]: r[1:] for r in hive("rank-totals", "siii")}
    summaries, iterations = {}, {}
    for wiki in LINKED_WIKIS:
        its = json.loads((data / "pagerank" / f"{wiki}-{a.dump}" / "iterations.json").read_text())
        iterations[wiki] = its
        n = its["articles"]
        rows, agree = join[wiki]
        count, distinct, viewed, rank_sum, user, automated, through = totals[wiki]
        rcount, rdistinct, rsum = rank_totals[wiki]
        c.check(f"hive linked rows, {wiki}", rows == agree == count == distinct == rcount == rdistinct == n,
                f"{fmt(n)} articles, each once in LinkedVsRead, the graph's titles and PageRank's ranks, "
                f"with the same title and rank in all three")
        mass = its["mass"] - its["iterations"][-1]["carry"]
        c.check(f"hive linked ranks, {wiki}", rank_sum == rsum == mass,
                f"{fmt(rank_sum)} units, PageRank's mass less its last carry")
        vt = counters(logs[f"linked {wiki}"])["view-targets"]
        fate = {ag: {f: vt.get((f"{ag} views", f), 0) for f in VIEW_FATES} for ag in AGENTS}
        c.check(f"hive linked views, {wiki}",
                user == fate["user"]["ARTICLE"] + fate["user"]["REDIRECT"]
                and automated == fate["automated"]["ARTICLE"] + fate["automated"]["REDIRECT"]
                and through == fate["user"]["REDIRECT"],
                f"{fmt(user)} user views reach an article, as ViewTargets counted, {fmt(through)} through a redirect")
        s = json.loads((data / "linked" / f"{wiki}-{a.dump}" / "linked-summary.json").read_text(encoding="utf-8"))
        summaries[wiki] = s
        hive_top = {m: [r[3] for r in top_rows if r[1] == wiki and r[0] == m]
                    for m in ("pagerank", "views")}
        ours = {"pagerank": [r["page_id"] for r in s["top_by_pagerank"]],
                "views": [r["page_id"] for r in s["top_by_views"]]}
        c.check(f"hive top lists, {wiki}", hive_top == ours and all(len(v) == 100 for v in ours.values()),
                "the top 100 by PageRank and by user views, the same pages in the same order from Hive as from "
                "check_linked.py (numpy)")

    hours = len(days) * 24
    ranked = [line.split("\t")[0] for line in
              (REPO / "mapreduce/src/main/resources/wikis.tsv").read_text(encoding="utf-8").splitlines()]
    main_pages = {line.split("\t")[0]: line.split("\t")[1] for line in
                  (REPO / "mapreduce/src/main/resources/wikis.tsv").read_text(encoding="utf-8").splitlines()}
    hourly = {(w, ag): [0] * hours for w in ranked for ag in AGENTS}
    seen = collections.Counter()
    for w, ag, d, h, v in hive("wiki-hour", "sssii"):
        i = days.index(d) * 24 + h
        hourly[(w, ag)][i] = v
        seen[(w, ag)] += 1
    c.check("hive hours", all(seen[k] == hours for k in hourly),
            f"{hours} hours of user and automated views for each of the {len(ranked)} ranked wikis")
    c.check("hive hours against days",
            all(sum(hourly[(w, ag)][i * 24:(i + 1) * 24]) == wiki_day[(w, ag, d)]
                for w in ranked for ag in AGENTS for i, d in enumerate(days)),
            "each day's 24 hours sum to its total")

    # 3. Runs repeated on purpose.
    repeats = []
    script = ('for r in clean rerun worker datanode nodemanager; do '
              'echo "$r $(hdfs dfs -cat /out/recovery/$r/part-r-* 2>/dev/null | sha256sum | cut -d" " -f1)"; done')
    rec = dict(line.split() for line in compose("exec", "-T", "namenode", "bash", "-c", script).splitlines() if line.strip())
    c.check("recovery", len(rec) == 5 and len(set(rec.values())) == 1,
            f"DomainHourTotals clean, rerun, and with a worker, a DataNode, a NodeManager killed: "
            f"{len(set(rec.values()))} distinct output")
    repeats.append({"what": "DomainHourTotals on 1 and 28 October (user), five runs: clean, rerun, a worker killed, "
                            "a DataNode killed, a NodeManager killed (docs/RECOVERY.md)",
                    "runs": rec, "identical": True})
    g1, g2 = data / "graphs" / f"simplewiki-{a.dump}-v1", data / "graphs" / f"simplewiki-{a.dump}"
    files = ("graph.adjacency.tsv", "graph.titles.tsv", "graph.redirects.tsv", "link-targets.hops.tsv")
    gh = {f: (sha256_file(g1 / f), sha256_file(g2 / f)) for f in files}
    c.check("graph built twice", all(x == y for x, y in gh.values()),
            "Simple English's graph, uncompressed and then with compressed shuffle and output: the same bytes")
    repeats.append({"what": "Simple English's link graph built twice, the second with compressed shuffle and output",
                    "runs": {f: x for f, (x, _) in gh.items()}, "identical": True})
    p1, p2 = data / "pagerank" / f"simplewiki-{a.dump}" / "run-20261108", data / "pagerank" / f"simplewiki-{a.dump}"
    i1, i2 = (json.loads((p / "iterations.json").read_text())["iterations"] for p in (p1, p2))
    keys = ("pool", "share", "carry", "change_of_previous", "link_mass")
    same_its = len(i1) == len(i2) and all(x[k] == y[k] for x, y in zip(i1, i2) for k in keys)
    r1, r2 = sha256_file(p1 / "ranks.tsv"), sha256_file(p2 / "ranks.tsv")
    c.check("pagerank run twice", r1 == r2 and same_its,
            f"Simple English, {len(i2)} iterations each: the same ranks byte for byte, and the same pool, share, "
            f"carry and change in every iteration")
    repeats.append({"what": "Simple English's PageRank run twice in full", "runs": {"ranks.tsv": r2},
                    "iterations": len(i2), "identical": True})

    for section in RULES.values():
        for key, (cls, const, value) in section.items():
            src = source_at(runs["window"]["commit"], cls)
            m = re.search(rf"static final double {const} = ([0-9.eE-]+);", src)
            c.check(f"rule {cls}.{const}", m is not None and float(m.group(1)) == value,
                    f"{value:g} in the source the window ran")

    # The files.
    def base(*names: str) -> dict:
        return {"schema_version": SCHEMA_VERSION, "window": window, "dump": a.dump,
                "runs": {n: runs[n] for n in names}}

    rules = {name: {k: v for k, (_, _, v) in section.items()} for name, section in RULES.items()}
    window_counters = counters(logs["window"])
    counts = {(r[0], r[1]): r[2:] for r in hive("spike-counts", "ssiiiiii")}
    sh = window_counters["spike-hours"]
    ts = window_counters["top-spikes"]
    total = [sum(v[i] for v in counts.values()) for i in range(6)]
    c.check("hive spikes", total[:3] == [sh.get(("wikihadoop.SpikeHours$Counts", k)) for k in ("SPIKES", "FLAGGED", "BURSTS")],
            f"{total[0]:,} spikes, {total[1]:,} flagged, {total[2]:,} bursts, as SpikeHours counted")
    kept = ts.get(("wikihadoop.TopSpikes$Counts", "SPIKES_READ"), 0) - ts.get(("wikihadoop.TopSpikes$Counts", "NOT_RANKED"), 0)
    c.check("hive ranked spikes", total[3] == kept,
            f"{total[3]:,} of them on articles by Wikis' rule in SQL, as TopSpikes counted ({total[4]:,} flagged, "
            f"{total[5]:,} bursts)")
    top = collections.defaultdict(lambda: collections.defaultdict(list))
    spike_cols = ("user_views", "baseline", "excess", "automated_views", "desktop", "mobile_web", "mobile_app",
                  "flatness", "dominant_access", "dominant_share", "wiki_share_of_it", "flags", "peak_share", "burst")

    def spike(r: list) -> dict:
        d = dict(zip(spike_cols, r[3:17]))
        d["hourly"] = [int(x) for x in r[17].split(",")]
        if len(d["hourly"]) != 24:
            raise SystemExit(f"{r[0]} {r[1]} {r[2]}: {len(d['hourly'])} hours: nothing written")
        return {"title": r[1], **d}

    spike_top = hive("top-spikes", "i" + SPIKE)
    for r in spike_top:
        top[r[1]][r[3]].append({"rank": int(r[0]), **spike(r[1:])})
    c.check("hive top spikes", len(spike_top) == ts.get(("wikihadoop.TopSpikes$Counts", "KEPT")),
            f"{len(spike_top):,} top spikes, as TopSpikes kept")
    reading = {
        **base("window", "hive"),
        "rule": {"spike": rules["spike"], "definition": SPIKE_RULE},
        "days": days,
        "wikis": [{"wiki": w, "main_page": main_pages[w],
                   "user": [wiki_day[(w, "user", d)] for d in days],
                   "automated": [wiki_day[(w, "automated", d)] for d in days],
                   "spikes": [counts.get((w, d), [0] * 6)[3] for d in days],
                   "flagged": [counts.get((w, d), [0] * 6)[4] for d in days],
                   "bursts": [counts.get((w, d), [0] * 6)[5] for d in days],
                   "top_spikes": {d: top[w][d] for d in days}} for w in ranked],
    }
    flagged = hive("flagged-spikes", SPIKE)
    bots = {
        **base("window", "hive"),
        "rule": {"looks_automated": {**rules["looks_automated"], "wiki_share_below": 0.5, "definition": AUTOMATED_RULE},
                 "burst": {**rules["burst"], "definition": BURST_RULE}},
        "hours": {"first": f"{days[0]}T00:00Z", "count": hours},
        "wikis": [{"wiki": w, "user": hourly[(w, "user")], "automated": hourly[(w, "automated")]} for w in ranked],
        "flagged": [{"wiki": r[0], "day": r[2], **spike(r)} for r in flagged],
    }
    c.check("flagged spikes", len(flagged) == total[4],
            f"{len(flagged):,} articles' spikes flagged, as many as the counts per wiki and day")

    en = summaries["enwiki"]
    mass_en = iterations["enwiki"]["mass"]
    tops = {"pagerank": en["top_by_pagerank"], "views": en["top_by_views"]}
    hist = [[r[1], r[2], r[3]] for r in hive("linked-histogram", "siii") if r[0] == "enwiki"]
    c.check("histogram", sum(x[2] for x in hist) == en["articles"], f"{en['articles']:,} articles in its bins")
    linked = {
        **base("month-views", "graph enwiki", "pagerank enwiki", "linked enwiki", "hive"),
        "wiki": en["wiki"], "articles": en["articles"], "articles_viewed": en["articles_viewed"],
        "user_views": en["user_views"], "mass": mass_en,
        "spearman": en["spearman"],
        "overlap": en["overlap"], "top": tops,
        "read_beyond_links": en["read_beyond_links"], "linked_beyond_reads": en["linked_beyond_reads"],
        "histogram": {"bin": 0.1, "x": "log10 of the rank over the starting rank, floored to the bin",
                      "y": "log10 of the user views, floored to the bin; null for none", "cells": hist},
    }

    pv_fates = re.search(r"enum Fate \{([^}]*)\}", source_at(runs["window"]["commit"], "PageviewLine")).group(1)
    pv_fates = [f.strip() for f in pv_fates.split(",")]
    domain_hours = views = 0
    with open(data / "runs" / a.run / "domain-hour-user.part.tsv", encoding="utf-8") as f:
        for line in f:
            domain_hours += 1
            views += int(line.rsplit("\t", 1)[1])
    checks = {
        **base(*runs),
        "checks": c.lines,
        "fates": {
            "pageviews": {ag: {f: window_counters[f"domain-hour-{ag}"].get(("wikihadoop.PageviewLine$Fate", f), 0)
                               for f in pv_fates} for ag in AGENTS},
            "links": {w: {f: sum(counters(logs[f"graph {w}"])[job].get(("wikihadoop.LinkFate", f), 0)
                                 for job in ("edges", "graph")) for f in LINK_FATES} for w in LINKED_WIKIS},
            "views": {w: summaries[w]["fates"] for w in LINKED_WIKIS},
        },
        "reconciliation": {"domain_hours": domain_hours, "user_views": views,
                           "against": "projectviews, every hourly file of the window"},
        "pagerank": {w: {k: iterations[w][k] for k in ("articles", "edges", "mass", "units_per_article", "damping")}
                     | {"iterations": [{k: i[k] for k in ("iteration", "seconds", "change_of_previous", "pool",
                                                          "share", "carry")} for i in iterations[w]["iterations"]]}
                     for w in LINKED_WIKIS},
        "repeats": repeats,
    }

    ledger = json.loads(hdfs_cat(f"/raw/pageviews/_ingest-{start:%Y-%m}.json"))
    window_files = sorted((v | {"hdfs": k} for k, v in ledger.items()
                           if re.search(r"date=(\d{4}-\d{2}-\d{2})/", k).group(1) in days), key=lambda v: v["hdfs"])
    downloads = json.loads((data / f"manifest-{start:%Y-%m}.json").read_text())
    projectviews = []
    for h in range(hours):
        t = datetime.combine(start, time()) + timedelta(hours=h + 1)  # a file is named for the hour's end
        rel = f"other/pageviews/{t:%Y}/{t:%Y-%m}/projectviews-{t:%Y%m%d-%H}0000"
        if rel in downloads:
            projectviews.append({"source": rel, "bytes": downloads[rel]["bytes"], "sha256": downloads[rel]["sha256"]})
    c.check("inputs", len(window_files) == 2 * len(days) and len(projectviews) == hours,
            f"{len(window_files)} pageview_complete files as ingested, {len(projectviews)} projectviews files as downloaded")
    dumps = {}
    for w in LINKED_WIKIS:
        led = json.loads(hdfs_cat(f"/raw/dumps/_ingest-{w}-{a.dump}.json"))
        dumps[w] = {t: {k: v[k] for k in ("source", "sha1", "content_sha256", "lines")} for t, v in sorted(led.items())}
    env = dict(line.split("=", 1) for line in (REPO / "cluster/hadoop.env").read_text().splitlines()
               if line and not line.startswith("#"))
    cluster = {
        "hadoop": "apache/hadoop:3.4.3 (Java 11)", "hive": "apache/hive:4.0.1 on Tez 0.10.4",
        "workers": 3, "each_worker": "a DataNode and a NodeManager",
        "nodemanager_memory_mb": int(env["YARN-SITE.XML_yarn.nodemanager.resource.memory-mb"]),
        "nodemanager_vcores": int(env["YARN-SITE.XML_yarn.nodemanager.resource.cpu-vcores"]),
        "map_memory_mb": int(env["MAPRED-SITE.XML_mapreduce.map.memory.mb"]),
        "reduce_memory_mb": int(env["MAPRED-SITE.XML_mapreduce.reduce.memory.mb"]),
        "replication": int(env["HDFS-SITE.XML_dfs.replication"]),
        "machine": "one machine, Docker Compose",
    }

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    written = {}
    for name, doc in (("reading", reading), ("bots", bots), ("linked", linked), ("checks", checks)):
        path = out / f"{name}.json"
        path.write_text(json.dumps(doc, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        written[f"{name}.json"] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
    manifest = {
        **base(*runs),
        "inputs": {"pageview_complete": window_files, "projectviews": projectviews, "dumps": dumps},
        "cluster": cluster, "credits": CREDITS, "files": written,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    for name, f in written.items():
        print(f"wrote {name}: {f['bytes']:,} bytes{' (over 5 MB: public/data/)' if f['bytes'] > 5_000_000 else ''}")
    print(f"wrote manifest.json: {(out / 'manifest.json').stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
