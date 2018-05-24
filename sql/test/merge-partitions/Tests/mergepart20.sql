CREATE MERGE TABLE testme (a int, b varchar(32)) PARTITION BY RANGE USING (md5(a));
SELECT column_id, expression FROM table_partitions;

CREATE TABLE subtable1 (a int, b varchar(32));
CREATE TABLE subtable2 (a int, b varchar(32));

ALTER TABLE testme ADD TABLE subtable1 AS PARTITION BETWEEN '00000000000000000000000000000000' AND '7fffffffffffffffffffffffffffffff';
ALTER TABLE testme ADD TABLE subtable2 AS PARTITION BETWEEN 'f0000000000000000000000000000000' AND 'ffffffffffffffffffffffffffffffff';

INSERT INTO testme VALUES (1, 'first'), (2000, 'second'), (3, 'third'), (4000, 'fourth');

SELECT a, b FROM testme;
SELECT a, b FROM sublimits1;
SELECT a, b FROM sublimits2;

ALTER TABLE testme DROP TABLE sublimits1;
ALTER TABLE testme DROP TABLE sublimits2;

DROP TABLE sublimits1;
DROP TABLE sublimits2;
DROP TABLE testme;

SELECT column_id, expression FROM table_partitions;
