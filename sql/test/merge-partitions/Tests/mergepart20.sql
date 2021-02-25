CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE USING (sys.md5(a));
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));
SELECT column_id, expression FROM table_partitions;

ALTER TABLE testme ADD TABLE subtable1 AS PARTITION FROM '00000000000000000000000000000000' TO '7fffffffffffffffffffffffffffffff';
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION FROM '80000000000000000000000000000000' TO 'ffffffffffffffffffffffffffffffff';

INSERT INTO testme VALUES (1, 'first'), (2000, 'second'), (3, 'third'), (4000, 'fourth');

SELECT a, b FROM testme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

ALTER TABLE testme DROP TABLE subtable1;
ALTER TABLE testme DROP TABLE subtable2;

DROP TABLE subtable1;
DROP TABLE subtable2;
DROP TABLE testme;

SELECT column_id, expression FROM table_partitions;

CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE USING (iamdummy(a)); --error

CREATE FUNCTION iamdummy(a int) RETURNS INT BEGIN RETURN a + 1; END;

CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE USING (sys.iamdummy(a));
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));
CREATE TABLE subtable3 (a int, b varchar(32));

ALTER TABLE testme ADD TABLE subtable1 AS PARTITION FROM 11 TO 20;
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION FROM 1 TO 10;
ALTER TABLE testme ADD TABLE subtable3 AS PARTITION FROM 'abc' TO 'cde'; --error

INSERT INTO testme VALUES (1, 'first'), (10, 'second'), (2, 'third'), (15, 'fourth');
INSERT INTO testme VALUES (12, 'this'), (6, 'not'), (50, 'ok'); --error

INSERT INTO subtable1 VALUES (12, 'sixth');
INSERT INTO subtable1 VALUES (2, 'seventh'); --error

SELECT a, b FROM testme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

ALTER TABLE testme DROP TABLE subtable1;
ALTER TABLE testme DROP TABLE subtable2;

DROP TABLE subtable1;
DROP TABLE subtable2;
DROP TABLE subtable3;
DROP TABLE testme;
DROP FUNCTION iamdummy;

CREATE FUNCTION iamdummy(a int, b int, c int) RETURNS INT BEGIN RETURN a + b + c; END;
CREATE MERGE TABLE testme(d int, e int, f int) PARTITION BY RANGE USING (sys.iamdummy(d, e, f));
SELECT column_id, expression FROM table_partitions;
DROP TABLE testme;
DROP FUNCTION iamdummy;

/* Testing bad expressions */

CREATE FUNCTION iamdummy(a int) RETURNS INT BEGIN RETURN SELECT a UNION ALL SELECT a; END;
CREATE MERGE TABLE testme(a int) PARTITION BY RANGE USING (sys.iamdummy(a));
CREATE TABLE subtable1 (a int);
ALTER TABLE testme ADD TABLE subtable1 AS PARTITION FROM RANGE MINVALUE TO RANGE MAXVALUE;
INSERT INTO testme VALUES (1); --error, more than one row

ALTER TABLE testme DROP TABLE subtable1;
DROP TABLE testme;
DROP TABLE subtable1;
DROP FUNCTION iamdummy;

CREATE MERGE TABLE testme(a int) PARTITION BY RANGE USING (SUM(a)); --error aggregations not allowed in expressions
CREATE MERGE TABLE testme(a int) PARTITION BY RANGE USING (AVG(a) OVER ()); --error window functions not not allowed in expressions
