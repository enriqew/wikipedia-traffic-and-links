"""Reconcile one day of pageviews across Wikimedia's three public datasets.

1. The hourly pageview dumps, summed per domain code and hour.
2. The hourly projectviews files, Wikimedia's own per-domain totals.
3. The day's pageview_complete user file, summed per wiki, access method and hour
   (complete_totals.py), mapped onto domain codes.

Two facts found in phase 0 make the three line up:

- An hourly file is stamped with the END of the hour it covers: pageviews-20261028-120000 holds
  hour 11 of pageview_complete (11:00 to 12:00 UTC). A day's 24 hours are therefore the files
  stamped 01 to 23 of that day and 00 of the next.
- In the hourly files, mobile-app views are counted under the mobile domain code (en.m), with
  mobile-web.

Usage: python scripts/phase0/reconcile.py 2026-10-28 <user totals csv>
Prints the summary and writes reconcile-YYYYMMDD.json beside the data.
"""

import csv
import json
import os
import sys
import tempfile
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import duckdb

# Domain-code suffix per project, as the hourly dumps write it: <lang>[.m][.<suffix>].
SUFFIX = {"wikipedia": "", "wikibooks": "b", "wiktionary": "d", "wikinews": "n", "wikiquote": "q",
          "wikisource": "s", "wikiversity": "v", "wikivoyage": "voy"}
# Projects without a language prefix: (desktop, mobile).
# Wikifunctions has no code of its own in the hourly files: its views sit under the empty domain "".
GLOBAL = {"wikidata": ("www.wd", "m.wd"), "mediawiki": ("www.w", "m.w"), "wikisource": ("www.s", "m.s"),
          "wikifunctions": ('""', '""')}


def domain_code(wiki: str, access: str) -> str | None:
    mobile = access in ("mobile-web", "mobile-app")
    if wiki in GLOBAL:
        return GLOBAL[wiki][mobile]
    lang, _, project = wiki.partition(".")
    if project == "wikimedia":
        return lang + (".m.m" if mobile else ".m")
    if project in SUFFIX:
        suffix = SUFFIX[project]
        return lang + (".m" if mobile else "") + (f".{suffix}" if suffix else "")
    return None


def hourly_files(month_dir: Path, day: date, prefix: str, ext: str) -> list[tuple[Path, int]]:
    """The 24 files covering the day's hours, with the hour each one holds."""
    nxt = day + timedelta(days=1)
    files = [(month_dir / f"{prefix}-{day:%Y%m%d}-{h:02d}0000{ext}", h - 1) for h in range(1, 24)]
    next_dir = month_dir.parent.parent / f"{nxt:%Y}/{nxt:%Y-%m}"
    files.append((next_dir / f"{prefix}-{nxt:%Y%m%d}-000000{ext}", 23))
    return files


def main() -> None:
    day = date.fromisoformat(sys.argv[1])
    user_csv = sys.argv[2]
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    month_dir = root / f"other/pageviews/{day:%Y}/{day:%Y-%m}"
    con = duckdb.connect()
    cols = "columns={'domain': 'VARCHAR', 'title': 'VARCHAR', 'views': 'BIGINT', 'bytes': 'BIGINT'}"
    opts = f"delim=' ', header=false, quote='', escape='', {cols}"

    def load(table: str, prefix: str, ext: str) -> None:
        parts = []
        for path, hour in hourly_files(month_dir, day, prefix, ext):
            if not path.exists():
                raise SystemExit(f"missing {path}")
            parts.append(f"select domain, {hour} as hour, sum(views) as views "
                         f"from read_csv('{path.as_posix()}', {opts}) group by all")
        con.execute(f"create table {table} as " + " union all ".join(parts))

    load("dump", "pageviews", ".gz")
    load("proj", "projectviews", "")

    mapped: Counter = Counter()
    unmapped: Counter = Counter()
    with open(user_csv, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            code = domain_code(r["wiki"], r["access"])
            if code is None:
                unmapped[r["wiki"]] += int(r["views"])
            else:
                mapped[(code, int(r["hour"]))] += int(r["views"])
    tmp = Path(tempfile.gettempdir()) / f"reconcile-user-{day:%Y%m%d}.csv"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write("domain,hour,views\n")
        for (code, hour), n in mapped.items():
            f.write(f"{code},{hour},{n}\n")
    con.execute(f"create table usr as select * from read_csv('{tmp.as_posix()}', header=true)")

    def row(sql: str) -> dict:
        cur = con.execute(sql)
        return dict(zip([d[0] for d in cur.description], cur.fetchone()))

    def compare(a: str, b: str) -> dict:
        return row(f"""
            select count(*) as domain_hours,
                   count(*) filter (where x.views = y.views) as equal,
                   count(*) filter (where x.views <> y.views) as different,
                   count(*) filter (where y.views is null) as only_first,
                   count(*) filter (where x.views is null) as only_second,
                   sum(coalesce(x.views, 0)) as first_views, sum(coalesce(y.views, 0)) as second_views,
                   sum(coalesce(x.views, 0)) filter (where y.views is null) as only_first_views,
                   sum(coalesce(y.views, 0)) filter (where x.views is null) as only_second_views
            from {a} x full join {b} y using (domain, hour)""")

    out = {
        "day": day.isoformat(),
        "dump_vs_projectviews": compare("dump", "proj"),
        "user_vs_dump": compare("usr", "dump"),
        "user_unmapped_views": sum(unmapped.values()),
        "user_unmapped_wikis": dict(unmapped.most_common(10)),
        "only_in_dump_largest": con.execute(
            "select domain, sum(views) from dump anti join usr using (domain, hour) group by 1 order by 2 desc limit 10"
        ).fetchall(),
        "different_largest": con.execute(
            "select domain, hour, u.views, d.views from usr u join dump d using (domain, hour) "
            "where u.views <> d.views order by abs(u.views - d.views) desc limit 10").fetchall(),
    }
    out = json.loads(json.dumps(out, default=int))
    (root / f"reconcile-{day:%Y%m%d}.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
