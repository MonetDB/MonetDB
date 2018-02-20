CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE (a);

SELECT COUNT(*) from range_partitions;
CREATE TABLE subtable (a int, b varchar(32));
ALTER TABLE testme ADD TABLE subtable AS PARTITION BETWEEN 2 AND 3;
SELECT COUNT(*) from range_partitions;

DROP TABLE subtable; --error
DROP TABLE testme;
SELECT COUNT(*) from range_partitions;
