SELECT NULL AS table_catalog, (SELECT s.name FROM sys.schemas s WHERE t.schema_id = s.id) AS table_schema FROM sys.tables t;
SELECT (SELECT s.name FROM sys.schemas s WHERE t.schema_id = s.id) AS table_schema, NULL AS table_catalog FROM sys.tables t;
