CREATE MERGE TABLE listparts (a int, b varchar(32)) PARTITION BY VALUES ON (a);
SELECT COUNT(*) from range_partitions;
SELECT COUNT(*) from value_partitions;

CREATE TABLE subtable1 (a int, b varchar(32));
ALTER TABLE listparts ADD TABLE subtable1 AS PARTITION IN ('1', '2', '3');
SELECT COUNT(*) from range_partitions;
SELECT COUNT(*) from value_partitions;

CREATE TABLE subtable2 (a int, b varchar(32));
ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION BETWEEN 5 AND 10; --error
ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION IN ('3', '4', '5'); --error
ALTER TABLE listparts ADD TABLE subtable2 AS PARTITION IN ('5', '6', '7');

SELECT COUNT(*) from range_partitions;
SELECT COUNT(*) from value_partitions;

DROP TABLE subtable1; --error
ALTER TABLE listparts DROP TABLE subtable1;
ALTER TABLE listparts DROP TABLE subtable2;
DROP TABLE listparts;
DROP TABLE subtable1;
DROP TABLE subtable2;

SELECT COUNT(*) from range_partitions;
SELECT COUNT(*) from value_partitions;
