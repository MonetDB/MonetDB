-- query has correct output
SELECT * FROM (
	SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM, tables.name AS TABLE_NAME,
	'SYSTEM TABLE' AS TABLE_TYPE, '' AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM,
	null AS TYPE_NAME, 'id' AS SELF_REFERENCING_COL_NAME, 'SYSTEM' AS REF_GENERATION
		FROM tables, schemas WHERE tables.schema_id = schemas.id AND tables.type = 1
	UNION ALL
	SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM, tables.name AS TABLE_NAME,
	'TABLE' AS TABLE_TYPE, '' AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM,
	null AS TYPE_NAME, 'id' AS SELF_REFERENCING_COL_NAME, 'SYSTEM' AS REF_GENERATION
		FROM tables, schemas WHERE tables.schema_id = schemas.id AND tables.type = 0
	UNION ALL
	SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM, tables.name AS TABLE_NAME,
	'VIEW' AS TABLE_TYPE, tables.query AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM,
	null AS TYPE_NAME, 'id' AS SELF_REFERENCING_COL_NAME, 'SYSTEM' AS REF_GENERATION
		FROM tables, schemas WHERE tables.schema_id = schemas.id AND tables.type = 2
) AS tables WHERE 1 = 1
	ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME;

-- query has correct output
SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM,
tables.name AS TABLE_NAME, keycolumns."column" AS COLUMN_NAME,
keys.type AS KEY_SEQ, keys.name AS PK_NAME
	FROM keys, keycolumns, tables, schemas
	WHERE keys.id = keycolumns.id AND keys.table_id = tables.id
		AND tables.schema_id = schemas.id
		AND keys.type = 0
	ORDER BY COLUMN_NAME;

-- query returns 1 row, incorrect, while there is a 1 = 0 condition
SELECT null AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME,
false AS NON_UNIQUE, '' AS INDEX_QUALIFIER, '' AS INDEX_NAME,
0 AS TYPE, 0 AS ORDINAL_POSITION, '' AS COLUMN_NAME,
'' AS ASC_OR_DESC, 0 AS CARDINALITY, 0 AS PAGES,
'' AS FILTER_CONDITION WHERE 1 = 0;
