CREATE MERGE TABLE updateme (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subt1 (a int, b varchar(32));
CREATE TABLE subt2 (a int, b varchar(32));

ALTER TABLE updateme ADD TABLE subt2 AS PARTITION BETWEEN '201' AND '300';

ALTER TABLE updateme SET TABLE subt1 AS PARTITION BETWEEN '1' AND '100'; --error
ALTER TABLE updateme ADD TABLE subt1 AS PARTITION BETWEEN '1' AND '100';
SELECT minimum, maximum, with_nulls FROM range_partitions;

INSERT INTO updateme VALUES (1, 'first');
INSERT INTO subt1 VALUES (1, 'first');

SELECT a, b FROM updateme;
SELECT a, b FROM subt1;
SELECT a, b FROM subt2;

ALTER TABLE updateme SET TABLE subt1 AS PARTITION BETWEEN '-100' AND '0'; --error
DELETE FROM updateme;
ALTER TABLE updateme SET TABLE subt1 AS PARTITION BETWEEN '-100' AND '0';
SELECT minimum, maximum, with_nulls FROM range_partitions;

INSERT INTO updateme VALUES (1, 'ups'); --error
INSERT INTO subt1 VALUES (1, 'ups'); --error

INSERT INTO updateme VALUES (0, 'second'), (201, 'other table');
INSERT INTO subt1 VALUES (0, 'second');

START TRANSACTION;
ALTER TABLE updateme SET TABLE subt1 AS PARTITION BETWEEN '0' AND '200';
SELECT minimum, maximum, with_nulls FROM range_partitions;
INSERT INTO updateme VALUES (199, 'third');
ROLLBACK;

SELECT minimum, maximum, with_nulls FROM range_partitions;
INSERT INTO updateme VALUES (150, 'fourth'); --error
INSERT INTO subt1 VALUES (150, 'fourth'); --error

INSERT INTO updateme VALUES (-50, 'fifth');
INSERT INTO subt1 VALUES (-50, 'fifth');

SELECT a, b FROM updateme;
SELECT a, b FROM subt1;
SELECT a, b FROM subt2;

ALTER TABLE updateme DROP TABLE subt1;
ALTER TABLE updateme DROP TABLE subt2;

DROP TABLE subt1;
DROP TABLE subt2;
DROP TABLE updateme;
