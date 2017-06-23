SELECT count(*) FROM sys.statistics;

ANALYZE sys;
SELECT count(*) FROM sys.statistics WHERE nils <> 0;
SELECT count(*) FROM sys.statistics WHERE nils <> 0 AND (minval = 'nil' OR maxval = 'nil');
SELECT count(*) FROM sys.statistics WHERE count > 0 AND (minval = 'nil' OR maxval = 'nil');

ANALYZE tmp;
SELECT count(*) FROM sys.statistics WHERE nils <> 0;
SELECT count(*) FROM sys.statistics WHERE nils <> 0 AND (minval = 'nil' OR maxval = 'nil');
SELECT count(*) FROM sys.statistics WHERE count > 0 AND (minval = 'nil' OR maxval = 'nil');

ANALYZE profiler;
SELECT count(*) FROM sys.statistics WHERE nils <> 0;
SELECT count(*) FROM sys.statistics WHERE nils <> 0 AND (minval = 'nil' OR maxval = 'nil');
SELECT count(*) FROM sys.statistics WHERE count > 0 AND (minval = 'nil' OR maxval = 'nil');

-- DELETE FROM sys.statistics;

