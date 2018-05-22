CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));

INSERT INTO subtable1 VALUES (1, 'one'), (2, 'two'), (3, 'three');
INSERT INTO subtable2 VALUES (11, 'eleven'), (12, 'twelve'), (13, 'thirteen');

ALTER TABLE testme ADD TABLE subtable1 AS PARTITION BETWEEN 1 AND 10;
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION BETWEEN 11 AND 20;

SELECT a, b FROM testme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

DELETE FROM testme;

SELECT a, b FROM testme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

INSERT INTO subtable1 VALUES (1, 'one'), (5, 'five'), (9, 'fifteen');
INSERT INTO subtable2 VALUES (11, 'eleven'), (12, 'twelve'), (13, 'thirteen');

DELETE FROM testme where a > 5;

SELECT a, b FROM testme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

INSERT INTO subtable1 VALUES (6, 'six');

TRUNCATE testme;

SELECT a, b FROM testme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

ALTER TABLE testme DROP TABLE subtable1;

INSERT INTO subtable1 VALUES (1, 'one'), (2, 'two'), (3, 'three');
INSERT INTO subtable2 VALUES (11, 'eleven'), (12, 'twelve'), (13, 'thirteen');

TRUNCATE testme;

SELECT a, b FROM testme;
SELECT a, b FROM subtable1;
SELECT a, b FROM subtable2;

ALTER TABLE testme DROP TABLE subtable2;

DROP TABLE testme;
DROP TABLE subtable1;
DROP TABLE subtable2;
