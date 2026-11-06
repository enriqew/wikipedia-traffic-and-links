# Phase 0: one day, no Hadoop

Day: Wednesday 28 October 2026, UTC. Everything below was measured on that day's files with the
scripts in `scripts/phase0/`, before any cluster exists. Its purpose is to fix what the MapReduce
jobs read, what they must reproduce, and how big a window is.

## Files

| Dataset | Files for the day | Size |
|---|---|---|
| `pageview_complete`, user | `pageviews-20261028-user.bz2` | 564.7 MB |
| `pageview_complete`, automated | `pageviews-20261028-automated.bz2` | 1,302.1 MB |
| Hourly page dumps | 24 × `pageviews-YYYYMMDD-HH0000.gz` | 1,156.7 MB |
| Hourly per-project totals | 24 × `projectviews-YYYYMMDD-HH0000` | 0.6 MB |
| Simple English Wikipedia | `page`, `pagelinks`, `linktarget`, `redirect` (`.sql.gz`) | 156.9 MB |

The `automated` file is larger than the `user` file: 1.30 GB against 0.56 GB on this day, and
1.26 to 1.51 GB against 0.46 to 0.56 GB on each of 28 to 31 October. A window is sized on both.

Every file's size, `Last-Modified` and sha256 are in the download manifest, which later phases
check against.

## Formats

**`pageview_complete`**, one row per page and access method, space-separated, six fields:

```
wiki_code title page_id access_method daily_total hourly
en.wikipedia Mohs_scale 7188 desktop 3 C1T2
```

`hourly` is a run of `<hour letter><count>` pairs, `A` = 00:00 to 01:00 UTC through `X` = 23:00 to
24:00. `page_id` is `null` when the title matched no page (2,214,920 user rows). Access methods are
`desktop`, `mobile-web` and `mobile-app`. The file is sorted by wiki code.

**Hourly dumps**, four fields: `domain_code title views 0`. The last field, response size, is
always 0. Domain codes are `<lang>[.m][.<project suffix>]`: `en`, `en.m`, `fr.b`, `fr.m.b`.

**`projectviews`**, the same four fields with `-` as the title: one total per domain code.

**SQL dumps**: `INSERT` statements with one tuple per line, so each line is one row, which is what a
line-oriented MapReduce input needs. Since the 2024 schema change, `pagelinks` is
`(pl_from, pl_from_namespace, pl_target_id)` and `pl_target_id` points into `linktarget`
`(lt_id, lt_namespace, lt_title)`. `redirect` is `(rd_from, rd_namespace, rd_title, …)`, a title,
which `page` resolves to an id.

## Every line accounted for

| File | Lines | Parsed | Views |
|---|---|---|---|
| user | 49,455,014 | 49,455,014 | 372,596,151 |
| automated | 123,975,901 | 123,975,901 | 347,279,313 |

No row was rejected: every row has six fields, a well-formed hourly run in ascending hours, and
hourly counts that sum to its daily total.

## The three datasets agree exactly

Two facts make them line up, and neither is in the readmes:

1. **An hourly file is stamped with the end of the hour it covers.** `pageviews-20261028-120000`
   holds hour `L` (11:00 to 12:00) of `pageview_complete`, not hour `M`. Compared page by page on
   English Wikipedia, hour `L` against that file matches all 889,416 desktop pages and 977,429
   mobile pages with no difference at all; hour `M` against it disagrees on most pages. A day is
   therefore the files stamped 01 to 23 of that day and 00 of the next.
2. **The hourly files count mobile-app views under the mobile domain code**, together with
   mobile-web (`en.m`). Wikifunctions has no code of its own and sits under the empty code `""`.

Read that way, for the whole day:

| Comparison | Domain-hours | Equal | Views |
|---|---|---|---|
| hourly dumps against `projectviews` | 42,778 | 42,778 | 372,596,151 both |
| `pageview_complete` user against the hourly dumps | 42,778 | 42,778 | 372,596,151 both |

Read naively, with each file as the hour it is named for, only 2,165 domain-hours were equal and the
day differed by 0.09%, almost all of it in the first and last hours. The check in the plan can
therefore be equality, not a tolerance: the jobs' user totals per domain and hour must equal both
public hourly datasets.

## Users and automated agents

On this day Wikimedia classified 347,279,313 views as automated against 372,596,151 as user: 48% of
the traffic that did not identify itself as a crawler. The split is Wikimedia's (its detection
model is private), not this project's.

| | Desktop | Mobile web | Mobile app |
|---|---|---|---|
| user | 137,502,314 | 227,149,584 | 7,944,253 |
| automated | 301,769,626 | 45,409,048 | 100,639 |

English Wikipedia alone: 190,691,102 user views, 75,390,448 automated.

## Simple English link graph

| Table | Rows |
|---|---|
| `page` (all namespaces) | 952,779 |
| `pagelinks` | 25,470,883 |
| `linktarget` | 3,287,865 |
| `redirect` | 134,502 |

Small enough for a single-machine reference PageRank, which is its role in the plan.

## Licences

- Pageview datasets: CC0 ("All Analytics datasets are available under the Creative Commons CC0
  dedication").
- Link tables: no Wikimedia page states a licence for `pagelinks`, `linktarget`, `page` or
  `redirect`. `legal.html` says "all information on Wikimedia projects may be freely shared, copied,
  remixed, and used for any purpose", and the Terms of Use license contributed text under CC BY-SA
  4.0 and waive database rights over facts. A score per title computed from links is facts, not
  text. The page credits Wikipedia and the dump date either way.

## The dumps server

It enforces the Wikimedia User-Agent policy and caps clients at three connections per IP. One
connection ran at 3.4 to 5.0 MB/s, so a day of `user` and `automated` takes about eight minutes.

## Consequences for the plan

- **Window size.** At late-October sizes, a month of `user` plus `automated` is about 59 GB
  compressed, about 177 GB at replication 3. A quarter would not fit on the disk available.
- **Reconciliation is exact.** The check compares the jobs' per-domain, per-hour user totals with
  the hourly dumps and `projectviews`, read as above, and fails on any difference.
- **Parsing is cheap; the cluster earns its place elsewhere.** One Python process parses the user
  day in under three minutes. The MapReduce work is the per-article series over the whole window,
  the joins between user and automated, and PageRank, not the parsing of one day.
