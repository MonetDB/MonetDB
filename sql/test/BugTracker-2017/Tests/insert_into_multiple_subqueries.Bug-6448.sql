CREATE TABLE test_table(x VARCHAR(10), y INTEGER);
INSERT INTO test_table VALUES ('test1', (SELECT 1)), ('test3', (SELECT 1));
SELECT * FROM test_table;
