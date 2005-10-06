CREATE TABLE test (aap int);
INSERT INTO test VALUES (1),(2),(null);
SELECT MIN(aap),MAX(aap),SUM(aap) FROM test;
