CREATE TABLE test (id INT, a INT, b INT);
INSERT INTO test VALUES (0, 1, 2);
SELECT COUNT(*) FROM test WHERE (b = 1 OR b = 2) AND (a NOT IN (3, 4) OR b <> 1) AND a = 1;
SELECT * FROM test WHERE (b = 1 OR b = 2) AND (a NOT IN (3, 4) OR b <> 1) AND a = 1;
drop table test;
