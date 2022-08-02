create table t ("end" timestamp, start timestamp, s text);
insert into t values ('2015-03-01 00:00:00.135000', '2015-03-01 00:18:00.258000', 'foo');
insert into t values ('2015-03-01 00:04:00.135000', '2015-03-01 00:22:00.258000', 'bar');
SELECT count(*) / ((max("end")-min("start")) / 60) FROM t GROUP BY s HAVING max("end")-min("start")<> 0;
drop table t;
