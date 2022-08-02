SELECT	null AS TABLE_CAT,
	schemas.name AS TABLE_SCHEM,
	tables.name AS TABLE_NAME,
	columns.name AS COLUMN_NAME,
	columns.type AS TYPE_NAME,
	columns.type_digits AS COLUMN_SIZE,
	columns.type_scale AS DECIMAL_DIGITS,
	0 AS BUFFER_LENGTH,
	10 AS NUM_PREC_RADIX,
	null AS nullable,
	null AS REMARKS,
	columns."default" AS COLUMN_DEF,
	0 AS SQL_DATA_TYPE,
	0 AS SQL_DATETIME_SUB,
	0 AS CHAR_OCTET_LENGTH,
	columns.number + 1 AS ORDINAL_POSITION,
	null AS SCOPE_CATALOG,
	null AS SCOPE_SCHEMA,
	null AS SCOPE_TABLE
FROM schemas, tables, columns
WHERE columns.table_id = tables.id
  AND tables.schema_id = schemas.id
  AND tables.system = true 
  AND tables.name IN ('args', 'columns', 'functions', 'idx',
    'objects', 'keys', 'modules', 'sequences')
ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;
