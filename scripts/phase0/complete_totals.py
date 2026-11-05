"""Sum one pageview_complete file per (wiki, access method, hour), and count every line's fate.

A row is: wiki_code title page_id access_method daily_total hourly, where hourly is a run of
<hour letter><count> pairs, A = hour 0 to X = hour 23. Every line ends as parsed or rejected
with a reason; parsed plus rejected equals the lines read. A parsed row's hourly counts must sum
to its daily total, or the row is rejected.

Usage: python scripts/phase0/complete_totals.py <pageviews-YYYYMMDD-agent.bz2> <out prefix>
Writes <out prefix>.totals.csv (wiki,access,hour,views) and <out prefix>.fates.json.
"""

import bz2
import json
import re
import sys
from collections import Counter

PAIR = re.compile(r"([A-X])(\d+)")
HOURLY = re.compile(r"(?:[A-X]\d+)+")


def main() -> None:
    src, out = sys.argv[1], sys.argv[2]
    totals: Counter = Counter()
    fates: Counter = Counter()
    access_values: Counter = Counter()
    null_page_id = 0
    lines = 0
    views = 0
    with bz2.open(src, "rt", encoding="utf-8", errors="strict", newline="\n") as f:
        for line in f:
            lines += 1
            fields = line.rstrip("\n").split(" ")
            if len(fields) != 6:
                fates[f"fields_{len(fields)}"] += 1
                continue
            wiki, _title, page_id, access, daily, hourly = fields
            if not daily.isdigit() or not HOURLY.fullmatch(hourly):
                fates["bad_counts"] += 1
                continue
            pairs = PAIR.findall(hourly)
            hours = [ord(h) - 65 for h, _ in pairs]
            if hours != sorted(set(hours)):
                fates["hours_out_of_order"] += 1
                continue
            counts = [int(c) for _, c in pairs]
            if sum(counts) != int(daily):
                fates["daily_not_sum_of_hours"] += 1
                continue
            fates["parsed"] += 1
            access_values[access] += 1
            if page_id == "null":
                null_page_id += 1
            views += int(daily)
            for hour, count in zip(hours, counts):
                totals[(wiki, access, hour)] += count
    if sum(fates.values()) != lines:
        raise SystemExit("conservation failed: fates do not sum to lines read")
    with open(out + ".totals.csv", "w", encoding="utf-8") as f:
        f.write("wiki,access,hour,views\n")
        for (wiki, access, hour), n in sorted(totals.items()):
            f.write(f"{wiki},{access},{hour},{n}\n")
    with open(out + ".fates.json", "w", encoding="utf-8") as f:
        json.dump({"file": src.replace("\\", "/").rsplit("/", 1)[-1], "lines": lines, "fates": dict(fates),
                   "views": views, "access": dict(access_values), "null_page_id": null_page_id}, f, indent=1)
    print(json.dumps({"lines": lines, "fates": dict(fates), "views": views, "access": dict(access_values)}))


if __name__ == "__main__":
    main()
