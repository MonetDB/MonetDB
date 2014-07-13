CREATE TABLE casebug(a int);
SELECT SUM(a) FROM casebug;
SELECT MIN(a) FROM casebug;
SELECT COALESCE(MIN(a), 0) FROM casebug;
drop table casebug;
