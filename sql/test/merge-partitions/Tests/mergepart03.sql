CREATE MERGE TABLE listparts (a int, b varchar(32)) PARTITION BY RANGE ON (a);
CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));

START TRANSACTION;
ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION BETWEEN '-4' AND '12';
SELECT COUNT(*) from range_partitions;
ROLLBACK;

INSERT INTO subtable2 VALUES (1, 'hello');

SELECT COUNT(*) from range_partitions;
ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN '-4' AND '12';
SELECT COUNT(*) from range_partitions;

ALTER TABLE listparts DROP TABLE subtable2;

INSERT INTO subtable2 VALUES (-5, 'oh no');
ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN '-1' AND 12000; --error

SELECT COUNT(*) from range_partitions;

DROP TABLE listparts;
DROP TABLE subtable1;
DROP TABLE subtable2;
