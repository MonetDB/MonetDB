SELECT count(*) = 0 FROM sys.statistics;

ANALYZE sys;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0 AND (minval IS NULL OR maxval IS NULL);
SELECT count(*) > 0 FROM sys.statistics WHERE count > 0 AND (minval IS NULL OR maxval IS NULL);

ANALYZE tmp;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0 AND (minval IS NULL OR maxval IS NULL);
SELECT count(*) > 0 FROM sys.statistics WHERE count > 0 AND (minval IS NULL OR maxval IS NULL);

ANALYZE profiler;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0;
SELECT count(*) > 0 FROM sys.statistics WHERE nils <> 0 AND (minval IS NULL OR maxval IS NULL);
SELECT count(*) > 0 FROM sys.statistics WHERE count > 0 AND (minval IS NULL OR maxval IS NULL);

select '~BeginVariableOutput~';
DELETE FROM sys.statistics;
select '~EndVariableOutput~';
