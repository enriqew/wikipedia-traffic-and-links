"""Reconcile the DomainHourTotals job's output for a month with Wikimedia's hourly projectviews.

The job's output is date<TAB>domain<TAB>hour<TAB>views. projectviews is Wikimedia's own total per
domain code and hour; on 28 October 2026 it equalled the hourly page dumps for every domain-hour
(docs/PHASE0.md), so it stands in for them here at a thousandth of the size. Each projectviews file
is stamped with the end of the hour it covers.

The check is equality: every (date, domain, hour) present on either side must carry the same views
on both. Exits non-zero otherwise.

Usage: python scripts/reconcile_month.py 2026-10 <job output .tsv> [--start YYYY-MM-DD] [--days N]
--start and --days limit the check to part of the month, for a partial run.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("month")
    p.add_argument("job_output")
    p.add_argument("--start")
    p.add_argument("--days", type=int)
    args = p.parse_args()
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    month_start = datetime.strptime(args.month, "%Y-%m")
    nxt = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    first = datetime.strptime(args.start, "%Y-%m-%d") if args.start else month_start
    days = (nxt - first).days if args.days is None else args.days

    con = duckdb.connect()
    cols = "columns={'domain': 'VARCHAR', 'title': 'VARCHAR', 'views': 'BIGINT', 'bytes': 'BIGINT'}"
    parts = []
    for h in range(days * 24):
        covered = first + timedelta(hours=h)  # the hour the file holds
        stamp = covered + timedelta(hours=1)  # the hour it is named for
        path = root / f"other/pageviews/{stamp:%Y}/{stamp:%Y-%m}/projectviews-{stamp:%Y%m%d-%H}0000"
        if not path.exists():
            raise SystemExit(f"missing {path}")
        parts.append(f"select '{covered:%Y-%m-%d}' as date, domain, {covered.hour} as hour, sum(views) as views "
                     f"from read_csv('{path.as_posix()}', delim=' ', header=false, quote='', escape='', {cols}) "
                     f"group by all")
    con.execute("create table proj as " + " union all ".join(parts))
    last_day = (first + timedelta(days=days - 1)).strftime("%Y-%m-%d")
    con.execute(f"""create table job as select * from read_csv('{Path(args.job_output).as_posix()}', delim='\t',
                    header=false, quote='', escape='',
                    columns={{'date': 'VARCHAR', 'domain': 'VARCHAR', 'hour': 'INT', 'views': 'BIGINT'}})
                    where date between '{first:%Y-%m-%d}' and '{last_day}'""")
    cur = con.execute("""
        select count(*) as domain_hours,
               count(*) filter (where j.views = p.views) as equal,
               count(*) filter (where j.views <> p.views) as different,
               count(*) filter (where p.views is null) as only_job,
               count(*) filter (where j.views is null) as only_projectviews,
               sum(coalesce(j.views, 0)) as job_views, sum(coalesce(p.views, 0)) as projectviews_views,
               count(distinct date) as days
        from job j full join proj p using (date, domain, hour)""")
    out = dict(zip([d[0] for d in cur.description], cur.fetchone()))
    out["worst"] = con.execute("""
        select date, domain, hour, j.views, p.views from job j full join proj p using (date, domain, hour)
        where j.views is distinct from p.views
        order by abs(coalesce(j.views, 0) - coalesce(p.views, 0)) desc limit 10""").fetchall()
    out = json.loads(json.dumps(out, default=int))
    print(json.dumps(out, indent=1))
    return 0 if out["equal"] == out["domain_hours"] else 1


if __name__ == "__main__":
    sys.exit(main())
