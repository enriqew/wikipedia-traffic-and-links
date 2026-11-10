# Export contract

`scripts/export.py` writes the data of the project's page: five JSON files, built from the outputs
of one window's runs and one dump's, and only once every check of those runs holds. A failed check
stops it before the first file is written.

```bash
export WH_DATA_DIR=/path/to/data
uv run python scripts/export.py oct 2026-10-01 31 20261101 --out export
```

It needs the cluster up and the runs it reads already done: `run-window.sh` and `run-month-views.sh`
over the window, and `run-graph.sh`, `run-pagerank.sh` and `run-linked.sh` for `enwiki` and
`simplewiki` on the dump. It runs `run-hive.sh` itself.

## What it checks first

1. **Each run's own checks**, run again: `check_window.py` (the user views per domain and hour equal
   `projectviews` for every hour of the window, pass one's totals equal `DomainHourTotals`, every
   spike came back with its hours), and `check_graph.py`, `check_pagerank.py` and `check_linked.py`
   for each wiki of the dump, against their one-machine references for Simple English.
2. **Hive**, on the results of `hive/results.sql` over the external tables of `hive/tables.sql`:
   - `MonthViews`' views per wiki and agent equal `DomainHourTotals`' over the wiki's two domain
     codes, and the job's own counters;
   - `SpikeCandidates`' totals per day, wiki and agent equal `DomainHourTotals`' over the wiki's two
     domain codes, once its Wikimedia chapter's desktop views are added: the domain codes count
     `pt.wikimedia.org`'s desktop views under `pt.m`, Portuguese Wikipedia's mobile code, and
     `ru.wikimedia.org`'s under `ru.m`;
   - `LinkedVsRead`, the graph's titles and PageRank's ranks hold the same articles, with the same
     title and rank in all three; the ranks sum to PageRank's mass less its last carry; the views
     sum to the first two fates of `ViewTargets`' counters;
   - the top 100 by PageRank and by user views are the same pages in the same order from Hive as
     from `check_linked.py` (numpy), which also gives every position beyond them and Spearman's
     correlation: in Hive those need a wiki's 7.26 million articles sorted in one reducer, which ran
     out of memory in the cluster's 2 GB containers;
   - the spikes, flagged spikes and bursts equal `SpikeHours`' counters; the spikes on articles,
     by `Wikis`' rule written again in SQL over `wikis.tsv` as the window's commit had it, equal
     what `TopSpikes` read less what it found not ranked; the top spikes are as many as it kept;
   - each ranked wiki has all the window's hours, and each day's hours sum to its total.
3. **Runs repeated on purpose**, compared now by sha256: `DomainHourTotals` five times under
   failure (`docs/RECOVERY.md`), Simple English's graph built twice, Simple English's PageRank run
   twice in full.
4. **The rules' constants** (`SpikeRule`, `LooksAutomated`, `Burst`) as the export states them,
   against the source at the window run's commit; and every run's log names a commit without
   uncommitted changes.

## The files

Every file carries `schema_version` (now 1), `window` (`start`, `end`, `days`), `dump` (the dump
date) and `runs`: for each run it draws from, the log it was read from, the commit, and the jar's
sha256 (for Hive, the sha256 of the two SQL files). A reader refuses a `schema_version` it does not
know. Views are pages served, not people; `user` and `automated` are Wikimedia's split.

| File | Holds |
|---|---|
| `manifest.json` | Every input: the window's `pageview_complete` files with the sha256 their ingest checked in HDFS, the `projectviews` files with the sha256 of their download, each dump table with its published sha1, the sha256 of its content and its line count. The cluster's layout, the credit lines the page prints, and the size and sha256 of the other four files |
| `reading.json` | `days`, and per ranked wiki (`wikis.tsv`): its main page, user and automated views per day (its own, from `SpikeCandidates`' totals, which key them by wiki), the spikes, flagged spikes and bursts on its articles per day, and each day's top ten spikes (rank, title, views, baseline, excess, views by access method, the rule's numbers, 24 hourly counts). The spike rule with its constants |
| `bots.json` | Per ranked wiki, user and automated views per hour of the window (`hours.first`, `hours.count`), from `DomainHourTotals` over its two domain codes (`codes`): where its mobile code also counts its Wikimedia chapter's desktop views, `shared` says so and gives the window's total of them; every spike on an article that the `LooksAutomated` rule flags, with the numbers it read. The rule and the burst mark, each with its constants and definition, labelled as this project's rule |
| `linked.json` | English Wikipedia: articles, articles viewed, user views, PageRank's mass; Spearman's correlation (scipy); the overlaps of the two top 100 and top 1,000; the top 100 by each measure, each article with both positions; the 50 outliers each way; every article in a histogram of tenth-of-a-decade bins of rank (as a multiple of the starting rank) and user views (`null` for none) |
| `checks.json` | Every check's line as its script printed it; the fates of every pageview line, link row and viewed title; the reconciliation's size; PageRank per wiki and iteration (change, pool, share, carry, seconds); the repeated runs with their sha256 |

A rank is in units of 10^-9 of an article's starting rank; `rank_share` is a rank over the mass.
A position is 1 for the largest, ties to the lower page id. A file over 5 MB goes to
`public/data/<slug>/` in the portfolio, the rest to `src/data/<slug>/`.

## A limit of the domain codes

The hourly datasets name a wiki by a domain code, and `X.m` is both Wikipedia `X`'s mobile site and
the Wikimedia chapter `X.wikimedia.org`'s desktop site. For Portuguese and Russian Wikipedia, whose
chapters have such sites, the wiki's hourly views here, and the wiki's views that the window's jobs
read from `DomainHourTotals` (the spike floor in `SpikeRule`, the hours `LooksAutomated` compares a
spike with), include the chapter's desktop views. The export measures them and the files carry them.

## Not hand-edited

The files are copied as they are. A change to what they hold is a change here, in the export and
in the page together, with `schema_version` raised when a field changes meaning or goes away.
