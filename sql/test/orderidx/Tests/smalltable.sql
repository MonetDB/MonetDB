-- test robustness against small tables
CREATE TABLE xtmp2(i integer);
SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx FROM storage WHERE "table"= 'xtmp2';
ALTER TABLE xtmp2 SET read only;
CREATE ORDERED INDEX sys_xtmp2_i_oidx ON xtmp2(i);
SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx FROM storage WHERE "table"= 'xtmp2';
SELECT * FROM xtmp2 wHERE i>=0 AND i<8;

CREATE TABLE xtmp3(i integer);
INSERT INTO xtmp3 VALUES(3);
SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx FROM storage WHERE "table"= 'xtmp3';
ALTER TABLE xtmp3 SET read only;
CREATE ORDERED INDEX sys_xtmp3_i_oidx ON xtmp3(i);
SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx from storage where "table"= 'xtmp3';
SELECT * FROM xtmp3 WHERE i>=0 AND i<8;

CREATE TABLE xtmp4(i integer);
INSERT INTO xtmp4 VALUES (3),(0),(2);
SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx FROM storage WHERE "table"= 'xtmp4';
ALTER TABLE xtmp4 SET read only;
CREATE ORDERED INDEX sys_xtmp4_i_oidx ON xtmp4(i);
SELECT schema, table, column, type, mode, count, hashes, phash, imprints, sorted, orderidx from storage where "table"= 'xtmp4';
SELECT * FROM xtmp4 WHERE i>=0 AND i<8;

DROP INDEX sys_xtmp2_i_oidx;
DROP INDEX sys_xtmp3_i_oidx;
DROP INDEX sys_xtmp4_i_oidx;
DROP TABLE xtmp2;
DROP TABLE xtmp3;
DROP TABLE xtmp4;
