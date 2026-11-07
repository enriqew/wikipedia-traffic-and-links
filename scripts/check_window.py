"""Check a run of scripts/run-window.sh, from the outputs it copied out of HDFS.

1. DomainHourTotals over the user files against Wikimedia's projectviews, per date, domain and
   hour: equal (skipped, and said so, when the window's projectviews are not all downloaded).
2. Pass one's totals against DomainHourTotals, per date and domain, for each agent: equal. The two
   jobs count the same rows by different routes (in-mapper combining against a shuffle).
3. Every spike from pass two came out of pass three with its hours, and no other did.
4. Every top spike is a content page of a ranked wiki, at most ten per wiki and day.
5. Every spike's peak-hour share and burst mark agree with its 24 hourly counts.

Usage: python scripts/check_window.py <run name> <first day YYYY-MM-DD> <days>
Exits non-zero if a check fails.
"""

import collections
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "phase0"))
from reconcile import domain_code  # noqa: E402  the mapping phase 0 established


def read(path: Path) -> list[list[str]]:
    with open(path, encoding="utf-8") as f:
        return [line.rstrip("\n").split("\t") for line in f if line.strip()]


def main() -> int:
    run, start, days = sys.argv[1], date.fromisoformat(sys.argv[2]), int(sys.argv[3])
    data = Path(os.environ.get("WH_DATA_DIR", "data"))
    out = data / "runs" / run
    window = {(start + timedelta(days=i)).isoformat() for i in range(days)}
    failures = []

    def check(name: str, ok: bool, detail: str) -> None:
        print(f"{'ok  ' if ok else 'FAIL'}  {name}: {detail}")
        if not ok:
            failures.append(name)

    user_dh = collections.Counter()
    user_dh_hour = collections.Counter()
    for d, domain, hour, views in read(out / "domain-hour-user.part.tsv"):
        user_dh[(d, domain)] += int(views)
        user_dh_hour[(d, domain, int(hour))] += int(views)
    auto_dh = collections.Counter()
    for d, domain, _, views in read(out / "domain-hour-automated.part.tsv"):
        auto_dh[(d, domain)] += int(views)
    check("window", {d for d, _ in user_dh} == window, f"{len({d for d, _ in user_dh})} days with user views")

    # 1. against projectviews
    proj = collections.Counter()
    missing = 0
    first = datetime.combine(start, datetime.min.time())
    for h in range(days * 24):
        covered = first + timedelta(hours=h)
        stamp = covered + timedelta(hours=1)  # a file is named for the end of the hour it covers
        path = data / f"other/pageviews/{stamp:%Y}/{stamp:%Y-%m}/projectviews-{stamp:%Y%m%d-%H}0000"
        if not path.exists():
            missing += 1
            continue
        for line in open(path, encoding="utf-8"):
            domain, _, views, _ = line.rstrip("\n").split(" ")
            proj[(covered.date().isoformat(), domain, covered.hour)] += int(views)
    if missing:
        print(f"skip  projectviews: {missing} of {days * 24} hourly files not downloaded yet")
    else:
        keys = set(proj) | set(user_dh_hour)
        equal = sum(1 for k in keys if proj[k] == user_dh_hour[k])
        check("projectviews", equal == len(keys),
              f"{equal:,} of {len(keys):,} domain-hours equal, {sum(user_dh_hour.values()):,} views")

    # 2. pass one's totals against DomainHourTotals
    totals = {"user": collections.Counter(), "automated": collections.Counter()}
    unmapped = collections.Counter()
    for d, wiki, agent, access, views in read(out / "candidates.totals.tsv"):
        code = domain_code(wiki, access)
        if code is None:
            unmapped[agent] += int(views)
        else:
            totals[agent][(d, code)] += int(views)
    # A wiki with no domain code is left out of DomainHourTotals' output but counted in its
    # UNMAPPED_VIEWS counter; pass one's unmapped views must equal that counter.
    job_unmapped = {}
    section = None
    for line in open(out / "jobs.log", encoding="utf-8"):
        if line.startswith("=== "):
            section = line[4:].strip()
        elif "UNMAPPED_VIEWS=" in line and section in ("domain-hour-user", "domain-hour-automated"):
            job_unmapped[section.rsplit("-", 1)[1]] = int(line.split("=")[1])
    for agent, reference in (("user", user_dh), ("automated", auto_dh)):
        keys = set(totals[agent]) | set(reference)
        equal = sum(1 for k in keys if totals[agent][k] == reference[k])
        expected = job_unmapped.get(agent, 0)
        check(f"totals {agent}", equal == len(keys) and unmapped[agent] == expected,
              f"{equal:,} of {len(keys):,} date-domains equal, {sum(reference.values()):,} views, "
              f"{unmapped[agent]:,} unmapped against {expected:,} in DomainHourTotals")

    # 3. every spike got its hours
    spikes = {tuple(f[:3]) for f in read(out / "article-days.spikes.tsv")}
    hours = {tuple(f[:3]) for f in read(out / "spike-hours.part.tsv")}
    check("spike hours", spikes == hours, f"{len(spikes):,} spikes, {len(hours):,} with hours")

    # 4. top spikes
    top = read(out / "top-spikes.part.tsv")
    ranked = {}  # wiki -> (main page, non-article namespace names), as Wikis.ranked reads them
    for line in open(Path(__file__).resolve().parent.parent / "mapreduce/src/main/resources/wikis.tsv",
                     encoding="utf-8"):
        wiki, main_page, prefixes = line.rstrip("\n").split("\t")
        ranked[wiki] = (main_page, set(prefixes.split("|")))

    def content_page(wiki: str, title: str) -> bool:
        if wiki not in ranked or title in ("-", ranked[wiki][0]):
            return False
        colon = title.find(":")
        return colon <= 0 or title[:colon].lower() not in ranked[wiki][1]

    per_key = collections.Counter((f[1], f[3]) for f in top)
    not_content = sum(1 for f in top if not content_page(f[1], f[2]))
    check("top spikes", not_content == 0 and max(per_key.values(), default=0) <= 10,
          f"{len(top):,} spikes over {len(per_key):,} wiki-days, {not_content:,} not a content page")

    # 5. peak-hour share and burst mark (Burst.SHARE in the jobs)
    spike_rows = read(out / "spike-hours.part.tsv")
    wrong = 0
    for f in spike_rows:
        hourly = [int(v) for v in f[17].split(",")]
        share = max(hourly) / sum(hourly) if sum(hourly) else 0.0
        # Java's %.4f and Python's can round a tie differently, so the share gets half a digit.
        if abs(float(f[15]) - share) > 0.00005 + 1e-12 or f[16] != ("burst" if share >= 0.9 else "-"):
            wrong += 1
    bursts = [f for f in spike_rows if f[16] == "burst"]
    check("bursts", wrong == 0, f"{len(bursts):,} bursts, {wrong:,} spikes whose mark disagrees with their hours")

    flagged = collections.Counter(f[14] for f in spike_rows)
    print(f"info  looks-automated flags across all spikes: {dict(flagged)}")
    print(f"info  bursts by looks-automated flag: {dict(collections.Counter(f[14] for f in bursts))}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
