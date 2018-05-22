CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);
SELECT COUNT(*) from table_partitions;
SELECT COUNT(*) from range_partitions;

CREATE TABLE subtable1 (a int, b varchar(32));
ALTER TABLE testme ADD TABLE subtable1 AS PARTITION BETWEEN 5 AND 10;
SELECT COUNT(*) from table_partitions;
SELECT COUNT(*) from range_partitions;
ALTER TABLE testme ADD TABLE subtable1 AS PARTITION BETWEEN 5 AND 10; --error

CREATE TABLE wrongtable (a int, b varchar(32), c real);
ALTER TABLE testme ADD TABLE wrongtable AS PARTITION BETWEEN 5 AND 6; --error

CREATE TABLE subtable2 (a int, b varchar(32));
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION IN ('0', '1', '2'); --error
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION BETWEEN 7 AND 9; --error
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION BETWEEN 5 AND 5; --error

DROP TABLE subtable1; --error

ALTER TABLE testme DROP TABLE subtable1;
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION BETWEEN 5 AND 5;
ALTER TABLE testme DROP TABLE subtable2;

SELECT COUNT(*) from table_partitions;
SELECT COUNT(*) from range_partitions;

DROP TABLE testme;
DROP TABLE subtable1;
DROP TABLE wrongtable;
DROP TABLE subtable2;

SELECT COUNT(*) from table_partitions;
SELECT COUNT(*) from range_partitions;
