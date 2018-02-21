CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE (a);
SELECT COUNT(*) from range_partitions;

CREATE TABLE subtable (a int, b varchar(32));
ALTER TABLE testme ADD TABLE subtable AS PARTITION BETWEEN 5 AND 10;
SELECT COUNT(*) from range_partitions;
ALTER TABLE testme ADD TABLE subtable AS PARTITION BETWEEN 5 AND 10; --error

CREATE TABLE wrongtable (a int, b varchar(32), c real);
ALTER TABLE testme ADD TABLE wrongtable AS PARTITION BETWEEN 5 AND 6; --error

CREATE TABLE conflictingtable (a int, b varchar(32));
ALTER TABLE testme ADD TABLE conflictingtable AS PARTITION IN ('0', '1', '2'); --error
ALTER TABLE testme ADD TABLE conflictingtable AS PARTITION BETWEEN 7 AND 9; --error

DROP TABLE subtable; --error
ALTER TABLE testme DROP TABLE subtable;
DROP TABLE testme;
DROP TABLE subtable;
DROP TABLE wrongtable;
DROP TABLE conflictingtable;
SELECT COUNT(*) from range_partitions;
