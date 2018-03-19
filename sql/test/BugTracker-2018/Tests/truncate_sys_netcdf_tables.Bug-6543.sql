SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_vars;
TRUNCATE TABLE sys.netcdf_vars;
SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_vars;

SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_vardim;
TRUNCATE TABLE sys.netcdf_vardim;
SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_vardim;

SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_files;
TRUNCATE TABLE sys.netcdf_files;
SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_files;

SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_dims;
TRUNCATE TABLE sys.netcdf_dims;
SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_dims;

SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_attrs;
TRUNCATE TABLE sys.netcdf_attrs;
SELECT (COUNT(*) > 0) AS has_rows FROM sys.netcdf_attrs;

