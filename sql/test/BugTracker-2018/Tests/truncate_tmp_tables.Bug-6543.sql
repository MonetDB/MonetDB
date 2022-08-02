-- add some content into tmp._tables, tmp._columns, tmp.keys, tmp.idxs and tmp.objects by creating a temp table with a Pkey
CREATE TEMP TABLE tmptable6543 (id int NOT NULL PRIMARY KEY, name VARCHAR(99) NOT NULL) ON COMMIT PRESERVE ROWS;

SELECT (COUNT(*) > 0) AS has_rows FROM tmp.triggers;
TRUNCATE TABLE tmp.triggers;
SELECT (COUNT(*) > 0) AS has_rows FROM tmp.triggers;

SELECT (COUNT(*) > 0) AS has_rows FROM tmp.objects;
TRUNCATE TABLE tmp.objects;
SELECT (COUNT(*) > 0) AS has_rows FROM tmp.objects;

SELECT (COUNT(*) > 0) AS has_rows FROM tmp.keys;
TRUNCATE TABLE tmp.keys;
SELECT (COUNT(*) > 0) AS has_rows FROM tmp.keys;

SELECT (COUNT(*) > 0) AS has_rows FROM tmp.idxs;
TRUNCATE TABLE tmp.idxs;
SELECT (COUNT(*) > 0) AS has_rows FROM tmp.idxs;

SELECT (COUNT(*) > 0) AS has_rows FROM tmp._tables;
TRUNCATE TABLE tmp._tables;
SELECT (COUNT(*) > 0) AS has_rows FROM tmp._tables;

SELECT (COUNT(*) > 0) AS has_rows FROM tmp._columns;
TRUNCATE TABLE tmp._columns;
SELECT (COUNT(*) > 0) AS has_rows FROM tmp._columns;

DROP TABLE tmptable6543;

