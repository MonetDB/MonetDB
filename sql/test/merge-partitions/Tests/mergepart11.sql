CREATE MERGE TABLE moveaccrosspartitions (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE sublimits1 (a int, b varchar(32));
CREATE TABLE sublimits2 (a int, b varchar(32));
CREATE TABLE sublimits3 (a int, b varchar(32));

ALTER TABLE moveaccrosspartitions ADD TABLE sublimits1 AS PARTITION BETWEEN 1 AND 100;
ALTER TABLE moveaccrosspartitions ADD TABLE sublimits2 AS PARTITION BETWEEN 101 AND 200;
ALTER TABLE moveaccrosspartitions ADD TABLE sublimits3 AS PARTITION BETWEEN 201 AND 300;

INSERT INTO moveaccrosspartitions VALUES (50, 'first'), (150, 'second'), (250, 'third'), (60, 'fourth'), (120, 'fifth'), (240, 'sixth');

SELECT a, b FROM moveaccrosspartitions;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;
SELECT a, b FROM sublimits3;

UPDATE moveaccrosspartitions SET a = a + 1 WHERE a % 50 = 0;

SELECT a, b FROM moveaccrosspartitions;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;
SELECT a, b FROM sublimits3;

UPDATE moveaccrosspartitions SET a = a - 50, b = 'p' || b || 's' WHERE a % 60 = 0;

SELECT a, b FROM moveaccrosspartitions;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;
SELECT a, b FROM sublimits3;

UPDATE moveaccrosspartitions SET a = a - 60 WHERE a % 10 = 0; --error

SELECT a, b FROM moveaccrosspartitions;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;
SELECT a, b FROM sublimits3;

UPDATE moveaccrosspartitions SET a = a + 100, b = 'moved' WHERE a % 10 = 0 AND a < 100;

SELECT a, b FROM moveaccrosspartitions;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;
SELECT a, b FROM sublimits3;

ALTER TABLE moveaccrosspartitions DROP TABLE sublimits1;
ALTER TABLE moveaccrosspartitions DROP TABLE sublimits2;
ALTER TABLE moveaccrosspartitions DROP TABLE sublimits3;

DROP TABLE sublimits1;
DROP TABLE sublimits2;
DROP TABLE sublimits3;
DROP TABLE moveaccrosspartitions;
