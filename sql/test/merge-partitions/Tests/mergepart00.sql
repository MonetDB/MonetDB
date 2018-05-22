SELECT COUNT(*) from table_partitions;
CREATE MERGE TABLE myfirstattempt (a int, b varchar(32)) PARTITION BY RANGE ON (d); --error
SELECT COUNT(*) from table_partitions;
CREATE MERGE TABLE mysecondattempt (a int, b varchar(32)) PARTITION BY RANGE ON (a);
SELECT COUNT(*) from table_partitions;
DROP TABLE mysecondattempt;
SELECT COUNT(*) from table_partitions;
