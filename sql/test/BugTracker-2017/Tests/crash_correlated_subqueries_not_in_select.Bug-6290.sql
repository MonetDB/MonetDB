SELECT s.name AS schema_nm, s.system AS is_system_schema
, (SELECT CAST(COUNT(*) as int) FROM sys.functions f WHERE f.schema_id = s.id AND f.type <> 2 AND f.id NOT IN (SELECT function_id FROM sys.systemfunctions)) AS "# user defined functions"
, (SELECT CAST(COUNT(*) as int) FROM sys.functions f WHERE f.schema_id = s.id AND f.type = 2 AND f.id NOT IN (SELECT function_id FROM sys.systemfunctions)) AS "# user defined procedures"
 FROM sys.schemas AS s
WHERE s.name IN ('json', 'profiler', 'sys');

