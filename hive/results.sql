-- The queries behind the export (plan section 16), over the tables of hive/tables.sql. Each writes
-- its result, sorted, as tab-separated text to its own directory under ${results} in HDFS;
-- scripts/run-hive.sh copies them out, and scripts/export.py checks them and builds the files of
-- the page from them. NULL is written as \N.

USE wh;
SET hive.exec.compress.output=false;

-- Checks. scripts/export.py compares each with another query or with the jobs' own counters.

-- MonthViews per wiki: titles, user and automated views.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/month-views-totals'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, COUNT(*), SUM(user_views), SUM(automated_views)
FROM month_views GROUP BY wiki ORDER BY wiki;

-- DomainHourTotals per wiki, agent and day, over the wiki's two domain codes.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/wiki-day'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, agent, day, SUM(views)
FROM wiki_hour GROUP BY wiki, agent, day ORDER BY wiki, agent, day;

-- SpikeCandidates' totals per wiki, agent and day, over the access methods.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/access-totals-day'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, agent, day, SUM(views)
FROM access_totals WHERE wiki IN (${hivevar:wikis}) GROUP BY wiki, agent, day ORDER BY wiki, agent, day;

-- LinkedVsRead against the graph's titles and PageRank's ranks, page by page: rows of the three
-- joined, and how many have all three with the same title and the same rank.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/linked-join'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT coalesce(l.wiki, t.wiki, r.wiki) AS w, COUNT(*),
       SUM(IF(l.page_id IS NOT NULL AND t.page_id IS NOT NULL AND r.page_id IS NOT NULL
              AND l.title = t.title AND l.`rank` = r.`rank`, 1, 0))
FROM linked l
FULL OUTER JOIN titles t ON l.wiki = t.wiki AND l.page_id = t.page_id
FULL OUTER JOIN ranks r ON coalesce(l.wiki, t.wiki) = r.wiki AND coalesce(l.page_id, t.page_id) = r.page_id
GROUP BY coalesce(l.wiki, t.wiki, r.wiki) ORDER BY w;

-- LinkedVsRead per wiki: articles, distinct page ids, articles viewed, and the sums of rank, user
-- views, automated views and user views through redirects.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/linked-totals'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, COUNT(*), COUNT(DISTINCT page_id), SUM(IF(user_views > 0, 1, 0)), SUM(`rank`),
       SUM(user_views), SUM(automated_views), SUM(user_through_redirects)
FROM linked GROUP BY wiki ORDER BY wiki;

-- PageRank's ranks per wiki: rows, distinct page ids, sum.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/rank-totals'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, COUNT(*), COUNT(DISTINCT page_id), SUM(`rank`) FROM ranks GROUP BY wiki ORDER BY wiki;

-- The page's data.

-- The ranked wikis' views per hour and agent, desktop and mobile together.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/wiki-hour'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, agent, day, hour, SUM(views)
FROM wiki_hour WHERE wiki IN (${hivevar:ranked})
GROUP BY wiki, agent, day, hour ORDER BY wiki, agent, day, hour;

-- The top ten spikes per ranked wiki and day, with their 24 hours.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/top-spikes'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM top_spikes ORDER BY wiki, day, `rank`;

-- Spikes per ranked wiki and day: all, flagged by LooksAutomated, marked as bursts; then the same
-- for the titles that rank (articles). Not vectorized: the vectorized plan of the ranked rule
-- (split and array_contains after an outer join) does not deserialize in the tasks (Kryo,
-- "unregistered class ID").
SET hive.vectorized.execution.enabled=false;
INSERT OVERWRITE DIRECTORY '${hivevar:results}/spike-counts'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, day, COUNT(*), SUM(IF(flags != '-', 1, 0)), SUM(IF(burst = 'burst', 1, 0)),
       SUM(IF(ranked, 1, 0)), SUM(IF(ranked AND flags != '-', 1, 0)), SUM(IF(ranked AND burst = 'burst', 1, 0))
FROM spike_hours_ranked GROUP BY wiki, day ORDER BY wiki, day;

-- Every article's spike LooksAutomated flags, with the numbers its rule read.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/flagged-spikes'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, title, day, user_views, baseline, excess, automated_views, desktop, mobile_web, mobile_app,
       flatness, dominant_access, dominant_share, wiki_share_of_it, flags, peak_share, burst, hourly
FROM spike_hours_ranked WHERE ranked AND flags != '-' ORDER BY wiki, day, title;
SET hive.vectorized.execution.enabled=true;

-- The top 100 articles by PageRank and by user views, ties to the lower page id. Each window is
-- filtered on its own position, so Hive keeps only a wiki's first 100 rows in the sort (top-n);
-- a position over all of a wiki's 7.26 million articles held them all in one reducer and ran out
-- of memory, so positions beyond the lists, and Spearman's correlation, are check_linked.py's.
INSERT OVERWRITE DIRECTORY '${hivevar:results}/linked-top'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT * FROM (
  SELECT 'pagerank' AS measure, wiki, pos, page_id, title, `rank`, user_views, automated_views, user_through_redirects
  FROM (SELECT *, row_number() OVER (PARTITION BY wiki ORDER BY `rank` DESC, page_id) AS pos FROM linked) a
  WHERE pos <= 100
  UNION ALL
  SELECT 'views' AS measure, wiki, pos, page_id, title, `rank`, user_views, automated_views, user_through_redirects
  FROM (SELECT *, row_number() OVER (PARTITION BY wiki ORDER BY user_views DESC, page_id) AS pos FROM linked) b
  WHERE pos <= 100) u
ORDER BY wiki, measure, pos;

-- Every article in bins of a tenth of a decade: of its rank as a multiple of the starting rank, and
-- of its user views (\N for an article with none).
INSERT OVERWRITE DIRECTORY '${hivevar:results}/linked-histogram'
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT wiki, rank_bin, views_bin, COUNT(*) FROM (
  SELECT wiki, CAST(floor(log10(`rank` / 1e9) * 10) AS INT) AS rank_bin,
         IF(user_views = 0, NULL, CAST(floor(log10(user_views) * 10) AS INT)) AS views_bin
  FROM linked) t
GROUP BY wiki, rank_bin, views_bin ORDER BY wiki, rank_bin, views_bin;
