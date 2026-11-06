# wiki-hadoop

MapReduce over Wikipedia's public traffic and link data, on a Hadoop 3.4 cluster that runs on one
machine. First what was read, split by who read it (users, and automated agents caught by
Wikimedia's bot detection); then PageRank over English Wikipedia's link graph, joined with the reads.

Work in progress. Done so far: phase 0 (one day, no Hadoop: formats, sizes, and an exact
reconciliation between Wikimedia's three public pageview datasets, in `docs/PHASE0.md`) and phase 1
(the cluster, the build, the tests, and a first job that reproduces that reconciliation).

## Layout

| Path | What |
|---|---|
| `compose.yaml`, `cluster/hadoop.env` | The cluster: NameNode, ResourceManager, JobHistory server, three workers each running a DataNode and a NodeManager |
| `mapreduce/` | The jobs, in Java 11 (the cluster's runtime), built with Maven inside Docker |
| `scripts/phase0/` | Phase 0 in Python: one day's download, per-day totals, the three-way reconciliation |
| `scripts/download.py`, `scripts/ingest.py` | A month's download, and its copy into HDFS verified by sha256 |
| `scripts/reconcile_month.py` | The job's user totals against Wikimedia's hourly projectviews, which must be equal |
| `scripts/mvn.sh` | Maven in a container, so no local JDK is needed |
| `scripts/audit_publishable.py` | Fails CI if a tracked file or commit message carries an address, a local path or a credential |

## Running it

```bash
export WH_DATA_DIR=/path/to/data          # downloads live here, outside the repository
python scripts/download.py 2026-10       # a month: pageview_complete and projectviews

docker compose up -d                      # the cluster
scripts/mvn.sh package                    # tests in the local job runner, then mapreduce/target/wiki-hadoop.jar

docker compose run --rm client bash -c '
  d=/raw/pageviews/agent=user/date=2026-10-28
  hdfs dfs -mkdir -p $d
  hdfs dfs -put /input/other/pageview_complete/2026/2026-10/pageviews-20261028-user.bz2 $d/
  hadoop jar /jobs/wiki-hadoop.jar wikihadoop.DomainHourTotals $d /out/domain-hour/user-20261028
  hdfs dfs -cat /out/domain-hour/user-20261028/part-r-00000' > domain-hour.tsv

uv run python scripts/reconcile_month.py 2026-10 domain-hour.tsv --start 2026-10-28 --days 1
```

The downloader keeps to one connection and sends a User-Agent with a contact, as the Wikimedia dumps
server requires (it caps clients at three connections per IP).

## Measured so far

On Wednesday 28 October 2026, `DomainHourTotals` on the cluster read 49,455,014 lines of the day's
`user` file, gave every one a fate (all parsed), and produced 42,778 domain-hour totals, all equal to
both of Wikimedia's hourly datasets: 372,596,151 views. The file sat in HDFS as five blocks at
replication 3, read by one map task per block (bzip2 splits; gzip would not). All seven map attempts
ran data-local; two of them were speculative duplicates, killed when the originals finished. The output's sha256 was the same before
and after the cluster was rebuilt with a different worker layout.

## Data

Pageview data: Wikimedia Foundation, CC0 ("All Analytics datasets are available under the Creative
Commons CC0 dedication").
