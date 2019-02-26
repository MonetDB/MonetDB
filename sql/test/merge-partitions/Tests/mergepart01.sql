CREATE SCHEMA other_schema;
CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE ON (a);
ALTER TABLE testme SET SCHEMA other_schema;
ALTER TABLE other_schema.testme SET SCHEMA sys;
ALTER TABLE testme RENAME TO testme2;
ALTER TABLE testme2 RENAME TO testme;

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
ALTER TABLE testme SET SCHEMA other_schema; --error, changing schema not allowed while with child tables
ALTER TABLE subtable1 SET SCHEMA other_schema; --error, changing the schema shouldn't be allowed while part of a merge table
ALTER TABLE subtable1 RENAME TO subtable3; --error, renaming the table shouldn't be allowed while part of a merge table

ALTER TABLE testme DROP TABLE subtable1;
ALTER TABLE subtable1 SET SCHEMA other_schema;
ALTER TABLE testme ADD TABLE other_schema.subtable1 AS PARTITION BETWEEN 4 AND 23; --error, all the tables must belong to the same schema
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION BETWEEN 5 AND 5;
ALTER TABLE testme DROP TABLE subtable2;

SELECT COUNT(*) from table_partitions;
SELECT COUNT(*) from range_partitions;

DROP TABLE testme;
DROP TABLE wrongtable;
DROP TABLE subtable2;
DROP SCHEMA other_schema CASCADE;

SELECT COUNT(*) from table_partitions;
SELECT COUNT(*) from range_partitions;
