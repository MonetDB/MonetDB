--create type "subject" as ("cid" varchar, "uri" varchar);
--
--create type "commit" as ("rev" varchar, "operation" varchar, "collection" varchar, "rkey" varchar, "record" json, "cid" varchar);
--
--create type event as ("did" varchar, "time_us" bigint, "kind" varchar, "commit" "commit");
--
--create table bluesky (data event);
--
---- this works
-- insert into bluesky select cast(t.json as event) from (select * from read_ndjson(r'/tmp/bluesky_nd.json')) t;
--
--select count(*) from bluesky;
--
-- select data."commit"."collection" from bluesky;


-- eats up first selection, only cnt column
--select e."commit"."collection" as ev, count(*) as cnt from (select cast(t.json as event) as e from (select json from read_ndjson(r'/tmp/bluesky_nd.json')) t) group by ev order by cnt desc;


-- column mapping lost
--SELECT e."commit"."collection" AS ev, count(*) AS cnt, count(distinct e."did") AS users FROM bluesky WHERE e."kind" = 'commit' AND e."commit"."operation" = 'create' GROUP BY ev ORDER BY cnt DESC;

-- 1 selection gone from results
-- Q1
SELECT data."commit"."collection" AS event, count(*) AS cnt FROM bluesky GROUP BY event ORDER BY cnt DESC;

-- Q2
SELECT data."commit"."collection" AS event, count(*) AS cnt, count(distinct data.did) AS users FROM bluesky WHERE data.kind = 'commit' AND data."commit"."operation" = 'create' GROUP BY event ORDER BY cnt DESC;

-- Q3
SELECT data."commit"."collection" AS event, "hour"(epoch(cast(data.time_us as bigint)/(1000*1000))) as hour_of_day, count(*) AS cnt FROM bluesky WHERE data.kind = 'commit' AND data."commit"."operation" = 'create' AND data."commit"."collection" in ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') GROUP BY event, hour_of_day ORDER BY hour_of_day, event;

-- Q4
SELECT data.did as user_id, epoch(cast(min(data.time_us) as bigint)/1000000) as first_post_ts FROM bluesky WHERE data.kind = 'commit' AND data."commit"."operation" = 'create' AND data."commit"."collection" = 'app.bsky.feed.post' GROUP BY user_id ORDER BY first_post_ts ASC LIMIT 3;

-- Q5
SELECT data.did as user_id, timestampdiff(epoch(cast(min(data.time_us) as bigint)/1000000),  epoch(cast(max(data.time_us) as bigint)/1000000))*1000 AS activity_span FROM bluesky WHERE data.kind = 'commit' AND data."commit"."operation" = 'create' AND data."commit"."collection" = 'app.bsky.feed.post' GROUP BY user_id ORDER BY activity_span DESC LIMIT 3;
