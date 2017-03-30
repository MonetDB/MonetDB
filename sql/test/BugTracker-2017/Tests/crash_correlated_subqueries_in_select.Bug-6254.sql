CREATE VIEW sys.view_stats AS
SELECT s.name AS schema_nm, s.id AS schema_id, t.name AS table_nm, t.id AS table_id, t.system AS is_system_view
, (SELECT CAST(COUNT(*) as int) FROM sys.columns c WHERE c.table_id = t.id) AS "# columns"
 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.id
WHERE query IS NOT NULL
; --ORDER BY s.name, t.name;

SELECT * FROM sys.view_stats;
-- prints worrying output in console
SELECT * FROM sys.view_stats WHERE is_system_view;
-- crash
SELECT * FROM sys.view_stats WHERE NOT is_system_view;
-- crash

DROP VIEW sys.view_stats;

