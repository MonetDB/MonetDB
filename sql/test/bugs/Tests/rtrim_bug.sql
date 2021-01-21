CREATE TABLE t1 (m varchar (1) NOT NULL);
INSERT into t1 values ('0');
INSERT into t1 values ('2');
SELECT * FROM sys.t1;
SELECT length("m") as data_length, "m" as data_value FROM "sys"."t1" WHERE "m" IS NOT NULL AND length("m") > 1;
-- no rows is expected

CREATE VIEW v1 as select "m" from t1 where m in (select m from sys.t1);
SELECT * FROM v1;
SELECT length("m") as data_length, "m" as data_value FROM "sys"."v1" WHERE "m" IS NOT NULL AND length("m") > 1;
-- no rows is expected

CREATE VIEW v2 as select "m" from t1 where rtrim(m) in (select rtrim(m) from sys.t1);
SELECT * FROM v2;

PLAN SELECT length("m") as data_length, "m" as data_value FROM "sys"."v2" WHERE "m" IS NOT NULL AND length("m") > 1;

set optimizer = 'sequential_pipe';
create procedure profiler.starttrace() external name profiler."starttrace";
create procedure profiler.stoptrace() external name profiler.stoptrace;

call profiler."starttrace"();
SELECT length("m") as data_length, "m" as data_value FROM "sys"."v2" WHERE "m" IS NOT NULL AND length("m") > 1;
call profiler.stoptrace();

select count(*) from sys.tracelog() where stmt like '% algebra.crossproduct%'; -- don't do crossjoin
select count(*) from sys.tracelog() where stmt like '% algebra.join%'; -- do inner join

drop procedure profiler.starttrace();
drop procedure profiler.stoptrace();
set optimizer = 'default_pipe';

SELECT length("m") as data_length, "m" as data_value FROM "sys"."v2" WHERE "m" IS NOT NULL AND length("m") > 1;
-- 2 rows returned !! should be 0 rows as with v1 !!
-- This query produces wrong results!!

DROP VIEW v1;
DROP VIEW v2;
DROP TABLE t1;

