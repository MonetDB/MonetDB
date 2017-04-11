DELETE FROM sys.statistics;

-- fill sys.statistics
ANALYZE sys.statistics;
SELECT /*column_id,*/ type, width, /*stamp,*/ "sample", count, "unique", nils, /*minval,*/ /*maxval,*/ sorted, revsorted FROM sys.statistics ORDER BY column_id;

-- update the values of: sample, count, unique, nils, minval, maxval, sorted, revsorted of sys.statistics
ANALYZE sys.statistics;
SELECT /*column_id,*/ type, width, /*stamp,*/ "sample", count, "unique", nils, /*minval,*/ /*maxval,*/ sorted, revsorted FROM sys.statistics ORDER BY column_id;

-- ALTER TABLE sys.statistics SET READ ONLY;

CREATE ORDERED INDEX stat_oidx ON sys.statistics (width);
SELECT /*column_id,*/ type, width, /*stamp,*/ "sample", count, "unique", nils, /*minval,*/ /*maxval,*/ sorted, revsorted FROM sys.statistics ORDER BY column_id;
-- now mserver5 is crashed !!

-- ALTER TABLE sys.statistics SET READ WRITE;

DELETE FROM sys.statistics;

