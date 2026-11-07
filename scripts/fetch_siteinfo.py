"""Fetch each wiki's namespace names and main page title, for the spike rankings.

Spikes are ranked among content pages only. A title in another namespace (File:, Category:, User:,
and each language's own names and aliases for them) and the wiki's main page stay in every total
but out of the rankings. MediaWiki publishes both through its siteinfo API; this writes them once
into mapreduce/src/main/resources/wikis.tsv, which ships inside the job jar:

    wiki_code <TAB> main_page_title <TAB> prefix|prefix|...

Titles use underscores for spaces, as the pageview files do. Prefixes are lower-cased; MediaWiki
namespace names are case-insensitive.

Usage: PYTHONIOENCODING=utf-8 python scripts/fetch_siteinfo.py
"""

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

USER_AGENT = "wiki-hadoop/0.1 (https://eredonda.com) python-urllib"
# The ten largest wikis by user views on 28 October 2026, 85% of the day's user views.
WIKIS = ["en", "ja", "de", "ru", "fr", "es", "it", "zh", "pt", "fa"]
OUT = Path(__file__).resolve().parent.parent / "mapreduce/src/main/resources/wikis.tsv"


def siteinfo(lang: str) -> dict:
    query = urllib.parse.urlencode({"action": "query", "meta": "siteinfo", "format": "json",
                                    "siprop": "general|namespaces|namespacealiases"})
    req = urllib.request.Request(f"https://{lang}.wikipedia.org/w/api.php?{query}",
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)["query"]


def main() -> None:
    rows = []
    for lang in WIKIS:
        q = siteinfo(lang)
        names = set()
        for ns in q["namespaces"].values():
            if ns["id"] == 0:
                continue
            for key in ("*", "canonical"):
                if ns.get(key):
                    names.add(ns[key])
        for alias in q["namespacealiases"]:
            if alias["id"] != 0:
                names.add(alias["*"])
        prefixes = sorted({n.replace(" ", "_").lower() for n in names})
        main_page = q["general"]["mainpage"].replace(" ", "_")
        rows.append(f"{lang}.wikipedia\t{main_page}\t{'|'.join(prefixes)}")
        print(f"{lang}: main page {main_page}, {len(prefixes)} namespace names")
        time.sleep(1)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(rows) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
