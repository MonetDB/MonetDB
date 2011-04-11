CREATE TABLE t1 (a INT, b INT);
INSERT INTO t1 VALUES (1, 3);
CREATE TABLE t2 (c INT, d INT);
INSERT INTO t2 VALUES (1, 4);
SELECT * FROM (SELECT * FROM t1 INNER JOIN t2 ON a = c) abc WHERE abc.a > abc.b;
drop table t1;
drop table t2;
