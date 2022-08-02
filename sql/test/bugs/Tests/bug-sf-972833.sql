CREATE TABLE test (x VARCHAR(10));
INSERT INTO test VALUES ('a');
INSERT INTO test VALUES ('b');

SELECT * FROM test WHERE x = 'a';
SELECT * FROM test WHERE x = 'a' AND x = 'b';

drop table test;
