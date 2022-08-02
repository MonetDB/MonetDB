start transaction;

CREATE TABLE test_table (d int);
INSERT INTO test_table VALUES (6),(6);
CREATE MERGE TABLE test_merge_table (t int);
ALTER TABLE test_merge_table ADD TABLE test_table;
SELECT * FROM test_merge_table;

ALTER TABLE test_table ADD COLUMN u int;
UPDATE test_table SET u = 2;
SELECT * FROM test_table;

ALTER TABLE test_table DROP COLUMN d;
SELECT * FROM test_merge_table;

rollback;
