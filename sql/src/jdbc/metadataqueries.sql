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

SELECT * FROM top LIMIT 15;