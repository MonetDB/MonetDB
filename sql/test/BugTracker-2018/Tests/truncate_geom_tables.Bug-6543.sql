SELECT (COUNT(*) > 0) AS has_rows FROM sys.spatial_ref_sys;
TRUNCATE TABLE sys.spatial_ref_sys;
SELECT (COUNT(*) > 0) AS has_rows FROM sys.spatial_ref_sys;

