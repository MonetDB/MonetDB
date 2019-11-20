START TRANSACTION;
CREATE SEQUENCE "my_test_seq" AS INTEGER START WITH 2;
CREATE TABLE altseqtest (t int default next value for "my_test_seq", v char);
INSERT INTO altseqtest(v)  VALUES ('a');
INSERT INTO altseqtest VALUES (10, 'b');
ALTER SEQUENCE "my_test_seq" RESTART WITH (SELECT max(t) + 1 FROM altseqtest);
INSERT INTO altseqtest(v)  VALUES ('c');
SELECT * FROM altseqtest;
ROLLBACK;

CREATE SEQUENCE dummyme START WITH 0 MINVALUE 1 MAXVALUE 30; --error, start value less than the minimum
CREATE SEQUENCE dummyme START WITH 31 MINVALUE 1 MAXVALUE 30; --error, start value higher than the maximum

CREATE SEQUENCE dummyme START WITH 2 MINVALUE 1 MAXVALUE 30;
ALTER SEQUENCE dummyme RESTART WITH 0; --error, cannot restart with a value lesser than the minimum
ALTER SEQUENCE dummyme RESTART WITH 31; --error, cannot restart with a value higher than the maximum
DROP SEQUENCE dummyme;
