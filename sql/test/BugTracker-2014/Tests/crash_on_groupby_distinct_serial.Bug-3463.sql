CREATE TABLE test (a int, b int, c serial);
SELECT a as t, count(distinct c) FROM test GROUP BY t;
drop table test;
