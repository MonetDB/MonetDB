set optimizer = 'sequential_pipe';

CREATE TABLE mytest(a HUGEINT, b HUGEINT, c HUGEINT, d HUGEINT, e HUGEINT);
INSERT INTO mytest VALUES (1234567890987654321, 10000000000, NULL, NULL, NULL);
explain UPDATE mytest SET a = 1234567890987654321;
explain UPDATE mytest SET b = 10000000000;
explain UPDATE mytest SET c = a * b;
explain UPDATE mytest SET d = c * b;
explain UPDATE mytest SET e = d + a;
        UPDATE mytest SET a = 1234567890987654321;
        UPDATE mytest SET b = 10000000000;
        UPDATE mytest SET c = a * b;
        UPDATE mytest SET d = c * b;
        UPDATE mytest SET e = d + a;
SELECT a, b, c, d, e from mytest;
DROP TABLE mytest;

SELECT 123456789098765432101234567890987654321;
START TRANSACTION;
CREATE TABLE sql_int128 (i HUGEINT);
explain INSERT INTO sql_int128 VALUES (123456789098765432101234567890987654321);
        INSERT INTO sql_int128 VALUES (123456789098765432101234567890987654321);
SELECT * FROM sql_int128;
ROLLBACK;

set optimizer = 'default_pipe';
