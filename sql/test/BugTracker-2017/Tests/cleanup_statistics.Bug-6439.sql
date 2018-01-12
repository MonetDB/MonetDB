CREATE TABLE sys.abc (a INT, b VARCHAR(10));
INSERT INTO sys.abc VALUES (1, 'one');
INSERT INTO sys.abc VALUES (2, 'two');
SELECT * FROM sys.abc;

DELETE FROM sys.statistics;

ANALYZE sys.abc;
SELECT /* column_id, */ type, width, /* stamp, */ "sample", "count", "unique", nils, minval, maxval, sorted, revsorted FROM sys.statistics;
-- expected 2 rows
SELECT /* column_id, */ type, width, /* stamp, */ "sample", "count", "unique", nils, minval, maxval, sorted, revsorted FROM sys.statistics where column_id not in (select id from sys.columns);
-- expected 0 rows

ALTER TABLE sys.abc DROP COLUMN b;
SELECT /* column_id, */ type, width, /* stamp, */ "sample", "count", "unique", nils, minval, maxval, sorted, revsorted FROM sys.statistics where column_id not in (select id from sys.columns);
-- expected 0 rows but found 1 row !

DROP TABLE sys.abc CASCADE;
SELECT /* column_id, */ type, width, /* stamp, */ "sample", "count", "unique", nils, minval, maxval, sorted, revsorted FROM sys.statistics where column_id not in (select id from sys.columns);
-- expected 0 rows but found 2 rows !

SELECT /* column_id, */ type, width, /* stamp, */ "sample", "count", "unique", nils, minval, maxval, sorted, revsorted FROM sys.statistics;
-- expected 0 rows but found 2 rows !
DELETE FROM sys.statistics;
