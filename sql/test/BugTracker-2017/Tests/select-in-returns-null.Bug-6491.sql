CREATE TABLE t(i INT);
INSERT INTO t VALUES (1), (2), (3);

SELECT 1 IN (SELECT i FROM t);
-- expect true

SELECT 4 IN (SELECT i FROM t);
-- expect false, got null
drop table t;
