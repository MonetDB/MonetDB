create table t ("end" timestamp, start timestamp, s text);
insert into t values ('2015-03-01 00:00:00.135000', '2015-03-01 00:18:00.258000', 'foo');
insert into t values ('2015-03-01 00:04:00.135000', '2015-03-01 00:22:00.258000', 'bar');
-- Update I changed the first division to a multiplication because it's not possible to divide a number with an interval
SELECT count(*) * ((max("end")-min("start")) / 60) FROM t GROUP BY s HAVING max("end")-min("start")<> interval '0' second;
drop table t;
