START TRANSACTION;
CREATE SEQUENCE "my_test_seq" AS INTEGER START WITH 2;
CREATE TABLE test (t int default next value for "my_test_seq", v char);
INSERT INTO test(v)  VALUES ('a');
INSERT INTO test VALUES (10, 'b');
ALTER SEQUENCE "my_test_seq" RESTART WITH (SELECT max(t) + 1 FROM test);
INSERT INTO test(v)  VALUES ('c');
SELECT * FROM test;
ROLLBACK;
