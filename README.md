# Wikipedia Traffic & Links

MapReduce over a month of Wikipedia's pageviews and English Wikipedia's link graph, on a Hadoop 3.4
cluster that runs on one machine. The jobs account for every line they read, their totals equal
Wikimedia's own, and PageRank over the link graph is set against what was opened.

The page built from this pipeline: <https://eredonda.com/projects/wikipedia-traffic-and-links?utm_source=github&utm_medium=referral&utm_campaign=wikipedia-traffic-and-links-repo>

## What it found

On October 2026's pageviews and the link tables dumped on 1 November 2026:

- English Wikipedia's top 100 articles by PageRank and its top 100 by user views share two,
  `United_States` and `YouTube`. Over all 7,260,699 articles Spearman's correlation is 0.54.
- The top of PageRank is `ISBN` (0.31% of the rank, 2,844th by views), then the other targets of
  citation and coordinate templates. The most viewed article after the main page, `Christa_Pike`,
  is 2,320,707th by PageRank.
- The month's 5,019,470,848 pageview lines all parse, and the user views per wiki and hour equal
  Wikimedia's hourly `projectviews` for all 1,418,374 domain-hours: 12,362,447,306 views.
- On the ten largest wikis, 32,918 days when an article's user views reached ten times its own
  baseline. A rule of this project's flags 573 of them as not looking like people reading; it does
  not call them bots.

A view is a page served, not a person. The split into user and automated views is Wikimedia's.

## How it is checked

| Check | What holds |
|---|---|
| Conservation | Every input line and row ends in one counted fate; a job fails unless they sum to what it read |
| Reconciliation | User views per domain and hour equal `projectviews`, once an hourly file is read as the hour it ends |
| PageRank mass | Ranks are integers and remainders go to a shared pool, so the total is the same integer after each of 53 iterations |
| References | On Simple English Wikipedia a one-machine Python script builds the graph, the ranks (NetworkX) and the views table again: equal by sha256, ranks within 0.006% |
| Determinism and recovery | Reruns give the same bytes, with a worker, a DataNode or a NodeManager killed mid-job too (`docs/RECOVERY.md`) |
| Hive | SQL over the job outputs gives the jobs' own counters |

`scripts/export.py` runs all of them, 78 checks, and writes the page's data only if every one holds
(`docs/EXPORT.md`).

## Layout

| Path | What |
|---|---|
| `compose.yaml`, `cluster/` | The cluster: NameNode, ResourceManager, JobHistory server, three workers each running a DataNode and a NodeManager; HiveServer2 4.0.1 on Tez as an optional service |
| `mapreduce/` | The jobs in Java 11, built and tested with Maven inside Docker; the tests run every job on fixtures in Hadoop's local job runner |
| `hive/` | External tables over the job outputs, and the queries behind the export |
| `scripts/download*.py`, `scripts/ingest*.py` | Downloads within the dumps server's limits, and the copy into HDFS, verified by checksum |
| `scripts/run-*.sh` | Each stage on the cluster: the pageview window, the month's views per title, the link graph, PageRank, views against PageRank, Hive |
| `scripts/check_*.py`, `scripts/reference_*.py` | The checks of each stage, and the one-machine reference implementations |
| `scripts/export.py` | The page's five JSON files |
| `scripts/audit_publishable.py` | Fails CI if a tracked file or commit message carries an address, a local path or a credential |
| `docs/` | Phase 0's findings on the data (`PHASE0.md`), the recovery tests (`RECOVERY.md`), the export contract (`EXPORT.md`) |

## Running it

Docker, Python 3.12 with `uv`, and disk: a month of pageviews is about 59 GB compressed and three
times that in HDFS at replication 3.

```bash
export WH_DATA_DIR=/path/to/data            # downloads and results live here, outside the repository
docker compose up -d                        # the cluster
scripts/mvn.sh package                      # tests, then mapreduce/target/wiki-hadoop.jar

uv run python scripts/download.py 2026-10                # pageview_complete and projectviews
docker compose run --rm client python3 /scripts/ingest.py 2026-10   # into HDFS, each sha256 read back
scripts/run-window.sh oct 2026-10-01 31                  # totals, spikes, the automated rule
scripts/run-month-views.sh oct 2026-10-01 31 en.wikipedia,simple.wikipedia

uv run python scripts/download_dumps.py enwiki 20261101  # page, linktarget, redirect, pagelinks
docker compose run --rm -T client python3 /scripts/ingest_dumps.py enwiki 20261101
scripts/run-graph.sh enwiki 20261101                     # four joins: the adjacency list
scripts/run-pagerank.sh enwiki 20261101
scripts/run-linked.sh enwiki 20261101 oct                # the month's views against the ranks

uv run python scripts/export.py oct 2026-10-01 31 20261101   # every check, then export/*.json
```

The same graph, PageRank and linked stages run for `simplewiki`, with
`scripts/reference_graph.py`, `reference_pagerank.py` and `reference_linked.py` as their oracles;
the export needs both wikis. Each script's header says what it reads and writes.

The downloader uses two connections by default and refuses more than three, the cap the Wikimedia
dumps server sets per IP, and sends a User-Agent with a contact, as the server asks.

## What it ran on

One desktop machine. Three workers with 6 GB and 3 cores each for tasks, 2 GB containers,
replication 3. The month's six pageview jobs took 2 h 34 min, English Wikipedia's graph 44 minutes,
and its PageRank 4 h 47 min for 53 iterations, each rereading 9.2 GB. These are what one machine
did, not a benchmark.

## Data

- Pageviews: Wikimedia Foundation, `pageview_complete` and `projectviews`
  (dumps.wikimedia.org/other/), CC0.
- Link graph: built from the English and Simple English Wikipedia database dumps (`page`,
  `pagelinks`, `linktarget`, `redirect`) of 1 November 2026, by Wikipedia's contributors. No
  Wikimedia page states a licence for those tables; the text they index is CC BY-SA 4.0, and what
  this project publishes from them is a score and a position per title.
