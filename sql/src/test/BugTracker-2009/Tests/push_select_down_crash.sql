
SELECT 
	'demo' AS "TABLE_CAT", 
	"schemas"."name" AS "TABLE_SCHEM", 
	"tables"."name" AS "TABLE_NAME", 
	"columns"."name" AS "COLUMN_NAME", 
	cast(CASE "columns"."type" 
	WHEN 'sec_interval' THEN -5 
	WHEN 'bigint' THEN -5 
	WHEN 'timetz' THEN 92 
	WHEN 'char' THEN 1 
	WHEN 'int' THEN 4 
	WHEN 'clob' THEN 2005 
	WHEN 'month_interval' THEN 4 
	WHEN 'table' THEN 2003 
	WHEN 'tinyint' THEN -6 
	WHEN 'date' THEN 91 
	WHEN 'double' THEN 8 
	WHEN 'decimal' THEN 3 
	WHEN 'timestamptz' THEN 93 
	WHEN 'blob' THEN 2004 
	WHEN 'timestamp' THEN 93 
	WHEN 'real' THEN 7 
	WHEN 'time' THEN 92 
	WHEN 'oid' THEN 1111 
	WHEN 'boolean' THEN 16 
	WHEN 'smallint' THEN 5 
	WHEN 'varchar' THEN 12 
	ELSE 1111 END AS smallint) AS "DATA_TYPE", 
	"columns"."type" AS "TYPE_NAME", 
	"columns"."type_digits" AS "COLUMN_SIZE", 
	"columns"."type_scale" AS "DECIMAL_DIGITS", 
	0 AS "BUFFER_LENGTH", 
	10 AS "NUM_PREC_RADIX", 
	cast(CASE "null" 
	WHEN true THEN 1 
	WHEN false THEN 0 END AS int) AS "NULLABLE", 
	cast(null AS varchar(1)) AS "REMARKS", 
	"columns"."default" AS "COLUMN_DEF", 
	0 AS "SQL_DATA_TYPE", 
	0 AS "SQL_DATETIME_SUB", 
	0 AS "CHAR_OCTET_LENGTH", 
	"columns"."number" + 1 AS "ORDINAL_POSITION", 
	cast(null AS varchar(1)) AS "SCOPE_CATALOG", 
	cast(null AS varchar(1)) AS "SCOPE_SCHEMA", 
	cast(null AS varchar(1)) AS "SCOPE_TABLE", 
	cast(1111 AS smallint) AS "SOURCE_DATA_TYPE", 
	CASE "null" 
	WHEN true THEN CAST ('YES' AS varchar(3)) 
	WHEN false THEN CAST ('NO' AS varchar(3)) END AS "IS_NULLABLE" 
FROM 
	"sys"."columns" AS "columns", 
	"sys"."tables" AS "tables", 
	"sys"."schemas" AS "schemas" 
WHERE 
	"columns"."table_id" = "tables"."id" AND 
	"tables"."schema_id" = "schemas"."id" AND 
	LOWER("schemas"."name") LIKE 'sys' AND 
	LOWER("tables"."name") LIKE 'r_repository_log' AND 
	LOWER("columns"."name") LIKE 'l1' 
ORDER BY 
	"TABLE_SCHEM", 
	"TABLE_NAME", 
	"ORDINAL_POSITION";
