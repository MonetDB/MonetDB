CREATE TABLE casebug(a int);
SELECT cast(SUM(a) as bigint) FROM casebug;
SELECT MIN(a) FROM casebug;
SELECT COALESCE(MIN(a), 0) FROM casebug;
drop table casebug;
