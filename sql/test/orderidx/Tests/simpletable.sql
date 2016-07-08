CREATE TABLE xtmp1(i integer);
INSERT INTO xtmp1 VALUES (1),(2),(4),(0),(10),(7),(3),(1),(1),(-4),(-9),(-1);
SELECT * FROM xtmp1;

SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx FROM storage WHERE "table" = 'xtmp1';
ALTER TABLE xtmp1 SET READ ONLY;
--call createorderindex('sys','xtmp1','i');
CREATE ORDERED INDEX sys_xtmp1_i_oidx ON xtmp1(i);

SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx FROM storage WHERE "table" = 'xtmp1';

SELECT * FROM xtmp1 WHERE i<0;
SELECT * FROM xtmp1 WHERE i<1;
SELECT * FROM xtmp1 WHERE i<2;
SELECT * FROM xtmp1 WHERE i<5;
SELECT * FROM xtmp1 WHERE i<8;

SELECT * FROM xtmp1 WHERE i>=0 AND i<8;
SELECT * FROM xtmp1 WHERE i>=2 AND i<=2;

--call droporderindex('sys','xtmp1','i');
DROP INDEX sys_xtmp1_i_oidx;

DROP TABLE xtmp1;
