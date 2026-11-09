-- External tables over the jobs' outputs in HDFS (plan section 16). No table owns its data: the
-- metastore is rebuilt from this file on every scripts/run-hive.sh, every table is external with
-- purge off, and dropping one deletes nothing in HDFS. The columns are the jobs' documented
-- outputs (each job's class comment).
--
-- A job with more than one named output writes them side by side in one directory (candidates-r-*
-- beside totals-r-*), and a table reads every file of its directory. Such a directory gets one
-- table of whole lines, and a view per output that keeps that output's files by INPUT__FILE__NAME
-- and splits the line.
--
-- Variables (scripts/run-hive.sh sets them):
--   run      the window's run (scripts/run-window.sh, run-month-views.sh): /out/<run>/
--   dump     the dump date of the graphs, PageRank and the linked tables, for enwiki and simplewiki
--   ranked   the ranked wikis of wikis.tsv, quoted and comma-separated
--   domains  their domain codes and Simple English's in DomainHourTotals' output, the same way

DROP DATABASE IF EXISTS wh CASCADE;
CREATE DATABASE wh;
USE wh;

-- DomainHourTotals, one partition per agent: date, domain code, hour, views.
CREATE EXTERNAL TABLE domain_hour (day STRING, domain STRING, hour INT, views BIGINT)
  PARTITIONED BY (agent STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
  TBLPROPERTIES ('external.table.purge'='false');
ALTER TABLE domain_hour ADD PARTITION (agent='user') LOCATION '/out/${hivevar:run}/domain-hour-user';
ALTER TABLE domain_hour ADD PARTITION (agent='automated') LOCATION '/out/${hivevar:run}/domain-hour-automated';

-- A wiki's views per hour: the ranked wikis and Simple English, each over its two domain codes
-- (the language code for desktop, code.m for mobile web and apps). The name is built from the code
-- only for the codes listed, which are all language codes of Wikipedia.
CREATE VIEW wiki_hour AS
SELECT agent, day, hour, concat(regexp_replace(domain, '\\.m$', ''), '.wikipedia') AS wiki,
       domain LIKE '%.m' AS mobile, views
FROM domain_hour WHERE domain IN (${hivevar:domains});

-- SpikeCandidates (pass one): candidates-r-* and totals-r-*.
CREATE EXTERNAL TABLE candidate_files (line STRING) STORED AS TEXTFILE
  LOCATION '/out/${hivevar:run}/candidates' TBLPROPERTIES ('external.table.purge'='false');
CREATE VIEW candidates AS
SELECT f[0] AS wiki, f[1] AS title, CAST(f[2] AS BIGINT) AS `rows`
FROM (SELECT split(line, '\t') AS f FROM candidate_files WHERE INPUT__FILE__NAME LIKE '%/candidates-r-%') t;
CREATE VIEW access_totals AS
SELECT f[0] AS day, f[1] AS wiki, f[2] AS agent, f[3] AS access, CAST(f[4] AS BIGINT) AS views
FROM (SELECT split(line, '\t') AS f FROM candidate_files WHERE INPUT__FILE__NAME LIKE '%/totals-r-%') t;

-- ArticleDays (pass two): spikes-r-* and articles-r-*.
CREATE EXTERNAL TABLE article_day_files (line STRING) STORED AS TEXTFILE
  LOCATION '/out/${hivevar:run}/article-days' TBLPROPERTIES ('external.table.purge'='false');
CREATE VIEW spikes AS
SELECT f[0] AS wiki, f[1] AS title, f[2] AS day, CAST(f[3] AS BIGINT) AS user_views,
       CAST(f[4] AS DOUBLE) AS baseline, CAST(f[5] AS DOUBLE) AS excess,
       CAST(f[6] AS BIGINT) AS automated_views, CAST(f[7] AS BIGINT) AS desktop,
       CAST(f[8] AS BIGINT) AS mobile_web, CAST(f[9] AS BIGINT) AS mobile_app
FROM (SELECT split(line, '\t') AS f FROM article_day_files WHERE INPUT__FILE__NAME LIKE '%/spikes-r-%') t;
CREATE VIEW spike_articles AS
SELECT f[0] AS wiki, f[1] AS title, CAST(f[2] AS BIGINT) AS user_views, CAST(f[3] AS BIGINT) AS automated_views
FROM (SELECT split(line, '\t') AS f FROM article_day_files WHERE INPUT__FILE__NAME LIKE '%/articles-r-%') t;

-- SpikeHours (pass three): each spike's line from pass two, then what LooksAutomated and Burst
-- made of its 24 hours, then the hours themselves.
CREATE EXTERNAL TABLE spike_hours (
  wiki STRING, title STRING, day STRING, user_views BIGINT, baseline DOUBLE, excess DOUBLE,
  automated_views BIGINT, desktop BIGINT, mobile_web BIGINT, mobile_app BIGINT,
  flatness DOUBLE, dominant_access STRING, dominant_share DOUBLE, wiki_share_of_it DOUBLE,
  flags STRING, peak_share DOUBLE, burst STRING, hourly STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
  LOCATION '/out/${hivevar:run}/spike-hours' TBLPROPERTIES ('external.table.purge'='false');

-- wikis.tsv as the window's jobs had it in their jar (scripts/run-hive.sh puts it in HDFS): each
-- ranked wiki, its main page, and its namespace names and aliases.
CREATE EXTERNAL TABLE wikis (wiki STRING, main_page STRING, prefixes STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
  LOCATION '/raw/wikis' TBLPROPERTIES ('external.table.purge'='false');

-- SpikeHours with whether the title ranks, by Wikis' rule (TopSpikes keeps only those): a ranked
-- wiki's title that is not its main page, not -, and not prefixed before its first colon by one of
-- its namespace names, compared in lower case.
CREATE VIEW spike_hours_ranked AS
SELECT s.*, (w.wiki IS NOT NULL AND s.title != w.main_page AND s.title != '-'
             AND NOT (instr(s.title, ':') > 1
                      AND array_contains(split(w.prefixes, '\\|'), lower(substr(s.title, 1, instr(s.title, ':') - 1)))))
            AS ranked
FROM spike_hours s LEFT JOIN wikis w ON s.wiki = w.wiki;

-- TopSpikes: a rank, then the spike's SpikeHours line.
CREATE EXTERNAL TABLE top_spikes (
  `rank` INT, wiki STRING, title STRING, day STRING, user_views BIGINT, baseline DOUBLE, excess DOUBLE,
  automated_views BIGINT, desktop BIGINT, mobile_web BIGINT, mobile_app BIGINT,
  flatness DOUBLE, dominant_access STRING, dominant_share DOUBLE, wiki_share_of_it DOUBLE,
  flags STRING, peak_share DOUBLE, burst STRING, hourly STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
  LOCATION '/out/${hivevar:run}/top-spikes' TBLPROPERTIES ('external.table.purge'='false');

-- MonthViews: each title's views over the window, per wiki (en.wikipedia, simple.wikipedia).
CREATE EXTERNAL TABLE month_views (wiki STRING, title STRING, user_views BIGINT, automated_views BIGINT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
  LOCATION '/out/${hivevar:run}/month-views' TBLPROPERTIES ('external.table.purge'='false');

-- The per-dump tables, one partition per wiki of the dump (enwiki, simplewiki).

-- Adjacency: adjacency-r-*, titles-r-* and redirects-r-*.
CREATE EXTERNAL TABLE graph_files (line STRING) PARTITIONED BY (wiki STRING) STORED AS TEXTFILE
  TBLPROPERTIES ('external.table.purge'='false');
ALTER TABLE graph_files ADD PARTITION (wiki='enwiki') LOCATION '/out/graph/enwiki-${hivevar:dump}/graph';
ALTER TABLE graph_files ADD PARTITION (wiki='simplewiki') LOCATION '/out/graph/simplewiki-${hivevar:dump}/graph';
CREATE VIEW titles AS
SELECT wiki, CAST(f[0] AS BIGINT) AS page_id, f[1] AS title
FROM (SELECT wiki, split(line, '\t') AS f FROM graph_files WHERE INPUT__FILE__NAME LIKE '%/titles-r-%') t;

-- PageRank's ranks: page id, rank in units of 10^-9 of an article's starting rank.
CREATE EXTERNAL TABLE ranks (page_id BIGINT, `rank` BIGINT) PARTITIONED BY (wiki STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
  TBLPROPERTIES ('external.table.purge'='false');
ALTER TABLE ranks ADD PARTITION (wiki='enwiki') LOCATION '/out/pagerank/enwiki-${hivevar:dump}/ranks';
ALTER TABLE ranks ADD PARTITION (wiki='simplewiki') LOCATION '/out/pagerank/simplewiki-${hivevar:dump}/ranks';

-- LinkedVsRead: every article with its rank and the window's views that reach it.
CREATE EXTERNAL TABLE linked (
  page_id BIGINT, title STRING, `rank` BIGINT, user_views BIGINT, automated_views BIGINT,
  user_through_redirects BIGINT)
  PARTITIONED BY (wiki STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
  TBLPROPERTIES ('external.table.purge'='false');
ALTER TABLE linked ADD PARTITION (wiki='enwiki') LOCATION '/out/linked/enwiki-${hivevar:dump}/linked';
ALTER TABLE linked ADD PARTITION (wiki='simplewiki') LOCATION '/out/linked/simplewiki-${hivevar:dump}/linked';
