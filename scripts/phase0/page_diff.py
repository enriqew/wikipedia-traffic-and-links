"""Compare one wiki's pages at one hour between pageview_complete (user) and the hourly dump.

The per-domain totals of the two datasets differ by about a tenth of a percent; this finds where,
page by page: titles only one side has, and titles both have with different counts.

Usage: python scripts/phase0/page_diff.py <user .bz2> <hourly dump .gz> <wiki> <lang> <hour>
e.g. ... pageviews-20261028-user.bz2 pageviews-20261028-120000.gz en.wikipedia en 12
"""

import bz2
import re
import sys
import tempfile
from pathlib import Path

import duckdb

PAIR = re.compile(r"([A-X])(\d+)")


def main() -> None:
    user_bz2, dump_gz, wiki, lang, hour = sys.argv[1:6]
    letter = chr(65 + int(hour))
    rows_path = Path(tempfile.gettempdir()) / f"page_diff-{lang}-{hour}.tsv"
    seen = False
    with bz2.open(user_bz2, "rt", encoding="utf-8", newline="\n") as f, \
            open(rows_path, "w", encoding="utf-8", newline="\n") as out:
        for line in f:
            if not line.startswith(wiki + " "):
                if seen:
                    break  # the file is sorted by wiki code, so this wiki is over
                continue
            seen = True
            _, title, page_id, access, _, hourly = line.rstrip("\n").split(" ")
            for h, c in PAIR.findall(hourly):
                if h == letter:
                    domain = lang if access == "desktop" else lang + ".m"
                    out.write(f"{domain}\t{access}\t{title}\t{page_id}\t{c}\n")
    con = duckdb.connect()
    con.execute(f"""create table u as select * from read_csv('{rows_path.as_posix()}', delim='\t', header=false,
                    quote='', escape='', columns={{'domain': 'VARCHAR', 'access': 'VARCHAR', 'title': 'VARCHAR',
                    'page_id': 'VARCHAR', 'views': 'BIGINT'}})""")
    con.execute(f"""create table d as select domain, title, views from read_csv('{dump_gz}', delim=' ', header=false,
                    quote='', escape='', columns={{'domain': 'VARCHAR', 'title': 'VARCHAR', 'views': 'BIGINT',
                    'bytes': 'BIGINT'}}) where domain in ('{lang}', '{lang}.m')""")
    con.execute("create table uu as select domain, title, sum(views) as views, "
                "bool_or(page_id = 'null') as null_id, string_agg(distinct access, '+') as access "
                "from u group by all")
    print(con.execute("""
        select domain,
               count(*) filter (where uu.views is not null and d.views is not null and uu.views = d.views) as same,
               count(*) filter (where uu.views is not null and d.views is not null and uu.views <> d.views) as differ,
               sum(d.views - uu.views) filter (where uu.views is not null and d.views is not null) as differ_views,
               count(*) filter (where d.views is null) as only_user, sum(uu.views) filter (where d.views is null) as only_user_views,
               count(*) filter (where uu.views is null) as only_dump, sum(d.views) filter (where uu.views is null) as only_dump_views
        from uu full join d using (domain, title) group by 1 order by 1""").fetchall())
    print("only in dump, largest:", con.execute("""
        select domain, title, d.views from d anti join uu using (domain, title) order by d.views desc limit 12""").fetchall())
    print("only in user, largest:", con.execute("""
        select domain, title, access, null_id, uu.views from uu anti join d using (domain, title)
        order by uu.views desc limit 12""").fetchall())
    print("differ, largest:", con.execute("""
        select domain, title, access, uu.views, d.views from uu join d using (domain, title)
        where uu.views <> d.views order by abs(d.views - uu.views) desc limit 12""").fetchall())


if __name__ == "__main__":
    main()
