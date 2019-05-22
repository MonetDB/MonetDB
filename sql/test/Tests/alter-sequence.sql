START TRANSACTION;
CREATE SEQUENCE "my_test_seq" AS INTEGER START WITH 2;
CREATE TABLE altseqtest (t int default next value for "my_test_seq", v char);
INSERT INTO altseqtest(v)  VALUES ('a');
INSERT INTO altseqtest VALUES (10, 'b');
ALTER SEQUENCE "my_test_seq" RESTART WITH (SELECT max(t) + 1 FROM altseqtest);
INSERT INTO altseqtest(v)  VALUES ('c');
SELECT * FROM altseqtest;
ROLLBACK;
