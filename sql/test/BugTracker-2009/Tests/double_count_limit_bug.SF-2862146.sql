create table t2 (i int);

SELECT COUNT (*) AS c1, COUNT (*) AS c2 FROM t2 limit 100;

SELECT COUNT (*) AS c1 FROM t2 limit 100;
SELECT COUNT (*) AS c1, COUNT (*) AS c2 FROM t2;

drop table t2;
