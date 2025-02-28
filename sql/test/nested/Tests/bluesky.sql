--create type "subject" as ("cid" varchar, "uri" varchar);
--
--create type "commit" as ("rev" varchar, "operation" varchar, "collection" varchar, "rkey" varchar, "record" json, "cid" varchar);
--
--create type event as ("did" varchar, "time_us" int, "kind" varchar, "commit" "commit");
--
--create table bluesky (e event);

-- this works
--insert into bluesky select cast(t.json as event) from (select json from read_nd_json(r'/tmp/bluesky_nd.json')) t;

-- column mapping lost
-- select e."commit"."collection" from bluesky;

-- this works
--select e."commit"."collection" as ev from (select cast(t.json as event) as e from (select json from read_nd_json(r'/tmp/bluesky_nd.json')) t);

-- eats up first selection, only cnt column
--select e."commit"."collection" as ev, count(*) as cnt from (select cast(t.json as event) as e from (select json from read_nd_json(r'/tmp/bluesky_nd.json')) t) group by ev order by cnt desc;


-- column mapping lost
--SELECT e."commit"."collection" AS ev, count(*) AS cnt, count(distinct e."did") AS users FROM bluesky WHERE e."kind" = 'commit' AND e."commit"."operation" = 'create' GROUP BY ev ORDER BY cnt DESC;

-- this crash server
-- SELECT e."commit"."collection" AS ev, count(*) AS cnt, count(distinct e."did") AS users FROM (select cast(t.json as event) as e from (select json from read_nd_json(r'/tmp/bluesky_nd.json')) t) WHERE e."kind" = 'commit' AND e."commit"."operation" = 'create' GROUP BY ev ORDER BY cnt DESC;

