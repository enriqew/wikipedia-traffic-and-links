"""Reconcile one day of pageviews across Wikimedia's three public datasets.

1. The 24 hourly pageview dumps, summed per domain code and hour.
2. The 24 hourly projectviews files, Wikimedia's own per-domain totals.
3. The day's pageview_complete user file, summed per wiki, access method and hour
   (complete_totals.py), mapped onto domain codes.

(1) against (2) asks whether the page-level dump adds up to the published totals. (3) against (1)
asks whether the daily per-agent file holds the same user traffic as the hourly dump, and which
access methods each domain code covers: whether mobile-app views sit in a desktop or a mobile
domain, or in neither, is not documented, so every candidate mapping is measured.

Usage: python scripts/phase0/reconcile.py 2026-10-28 <user totals csv>
Prints the summary and writes reconcile-YYYYMMDD.json beside the data.
"""

import json
import os
import sys
from datetime import date
from pathlib import Path

import duckdb

# Domain-code suffix for each project, as the hourly dumps write it: <lang>[.m][.<suffix>].
SUFFIX = {"wikipedia": "", "wikibooks": "b", "wiktionary": "d", "wikinews": "n", "wikiquote": "q",
          "wikisource": "s", "wikiversity": "v", "wikivoyage": "voy"}


def main() -> None:
    day = date.fromisoformat(sys.argv[1])
    user_csv = sys.argv[2]
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    month_dir = (root / f"other/pageviews/{day:%Y}/{day:%Y-%m}").as_posix()
    ymd = f"{day:%Y%m%d}"
    con = duckdb.connect()
    opts = "delim=' ', header=false, quote='', escape='', filename=true"

    con.execute(f"""
        create table dump as
        select domain, cast(substr(regexp_extract(filename, '-(\\d{{2}})0000\\.gz$', 1), 1, 2) as int) as hour,
               sum(views) as views, count(*) as rows
        from read_csv('{month_dir}/pageviews-{ymd}-*.gz', {opts},
                      columns={{'domain': 'VARCHAR', 'title': 'VARCHAR', 'views': 'BIGINT', 'bytes': 'BIGINT'}})
        group by all""")
    con.execute(f"""
        create table proj as
        select domain, cast(regexp_extract(filename, '-(\\d{{2}})0000$', 1) as int) as hour, sum(views) as views
        from read_csv('{month_dir}/projectviews-{ymd}-*', {opts},
                      columns={{'domain': 'VARCHAR', 'title': 'VARCHAR', 'views': 'BIGINT', 'bytes': 'BIGINT'}})
        group by all""")
    con.execute(f"create table cu as select * from read_csv('{user_csv}', header=true)")

    def row(sql: str) -> dict:
        cur = con.execute(sql)
        return dict(zip([d[0] for d in cur.description], cur.fetchone()))

    out: dict = {"day": day.isoformat()}

    # (1) against (2)
    out["dump_vs_projectviews"] = row("""
        select count(*) as domain_hours,
               count(*) filter (where coalesce(d.views, -1) = coalesce(p.views, -1)) as equal,
               sum(coalesce(d.views, 0)) as dump_views, sum(coalesce(p.views, 0)) as projectviews_views,
               count(*) filter (where d.views is null) as only_in_projectviews,
               count(*) filter (where p.views is null) as only_in_dump
        from dump d full join proj p using (domain, hour)""")

    # (3) mapped onto domain codes, one candidate mapping per treatment of mobile-app
    cases = " ".join(f"when '{p}' then '{s}'" for p, s in SUFFIX.items())
    con.execute(f"""
        create table cu_mapped as
        select split_part(wiki, '.', 1) as lang, case split_part(wiki, '.', 2) {cases} end as suffix,
               access, hour, views
        from cu where split_part(wiki, '.', 2) in ({", ".join(f"'{p}'" for p in SUFFIX)})""")
    out["user_views"] = con.execute("select sum(views) from cu").fetchone()[0]
    out["user_views_mapped"] = con.execute("select sum(views) from cu_mapped").fetchone()[0]
    out["user_views_by_access"] = dict(con.execute(
        "select access, sum(views) from cu group by 1 order by 2 desc").fetchall())

    def domain(mobile: str) -> str:
        return f"""lang || {mobile} || case when suffix = '' then '' else '.' || suffix end"""

    mappings = {
        "app_in_neither": f"""select case when access = 'desktop' then {domain("''")}
                                          when access = 'mobile-web' then {domain("'.m'")} end as domain, hour, views
                               from cu_mapped where access in ('desktop', 'mobile-web')""",
        "app_in_desktop": f"""select case when access = 'mobile-web' then {domain("'.m'")} else {domain("''")} end
                                     as domain, hour, views from cu_mapped""",
        "app_in_mobile": f"""select case when access = 'desktop' then {domain("''")} else {domain("'.m'")} end
                                    as domain, hour, views from cu_mapped""",
    }
    out["user_vs_dump"] = {}
    for name, sql in mappings.items():
        out["user_vs_dump"][name] = row(f"""
            with m as (select domain, hour, sum(views) as views from ({sql}) group by all),
                 d as (select domain, hour, views from dump
                       where domain in (select distinct domain from m))
            select count(*) as domain_hours,
                   count(*) filter (where coalesce(m.views, -1) = coalesce(d.views, -1)) as equal,
                   sum(coalesce(m.views, 0)) as user_views, sum(coalesce(d.views, 0)) as dump_views
            from m full join d using (domain, hour)""")

    # The largest wikis, per hour: where any difference sits.
    out["largest"] = con.execute("""
        with m as (select case when access = 'mobile-web' then lang || '.m' else lang end as domain, access, hour,
                          sum(views) as views
                   from cu_mapped where suffix = '' and lang in ('en', 'de', 'ja', 'es', 'fr', 'ru')
                   group by all)
        select m.domain, m.access, sum(m.views) as user_views,
               (select sum(views) from dump d where d.domain = m.domain) as dump_views
        from m group by all order by 1, 2""").fetchall()

    out = json.loads(json.dumps(out, default=int))
    (root / f"reconcile-{ymd}.json").write_text(json.dumps(out, indent=1))
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
