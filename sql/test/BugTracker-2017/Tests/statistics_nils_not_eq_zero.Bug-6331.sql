SELECT count(*) = 0 FROM sys.statistics;

ANALYZE sys;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0 AND (minval = 'nil' OR maxval = 'nil');
SELECT count(*) > 0 FROM sys.statistics WHERE count > 0 AND (minval = 'nil' OR maxval = 'nil');

ANALYZE tmp;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0 AND (minval = 'nil' OR maxval = 'nil');
SELECT count(*) > 0 FROM sys.statistics WHERE count > 0 AND (minval = 'nil' OR maxval = 'nil');

ANALYZE profiler;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0 AND (minval = 'nil' OR maxval = 'nil');
SELECT count(*) > 0 FROM sys.statistics WHERE count > 0 AND (minval = 'nil' OR maxval = 'nil');

-- DELETE FROM sys.statistics;

