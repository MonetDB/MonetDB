CREATE TABLE test (c UUID);
INSERT INTO test (SELECT CAST('1e1a9c62-d656-11e5-9fd7-9b884ad020cd' AS UUID) FROM generate_series(CAST(0 AS INTEGER), 200000, 1));
SELECT MIN(c) AS mn, MAX(c) AS mx FROM test;
SELECT MIN(c) AS mn, MAX(c) AS mx FROM test;
SELECT MIN(c) AS mn, MAX(c) AS mx FROM test;
DROP TABLE test;

