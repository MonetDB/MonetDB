-- getTables
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
	UNION ALL
	SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM, tables.name AS TABLE_NAME,
	'SESSION TEMPORARY TABLE' AS TABLE_TYPE, tables.query AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM,
	null AS TYPE_NAME, 'id' AS SELF_REFERENCING_COL_NAME, 'SYSTEM' AS REF_GENERATION
		FROM tables, schemas WHERE tables.schema_id = schemas.id AND tables.type = 3
	UNION ALL
	SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM, tables.name AS TABLE_NAME,
	'TEMPORARY TABLE' AS TABLE_TYPE, tables.query AS REMARKS, null AS TYPE_CAT, null AS TYPE_SCHEM,
	null AS TYPE_NAME, 'id' AS SELF_REFERENCING_COL_NAME, 'SYSTEM' AS REF_GENERATION
		FROM tables, schemas WHERE tables.schema_id = schemas.id AND tables.type = 4
) AS tables WHERE 1 = 1
	ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME;

-- getSchemas
SELECT name AS TABLE_SCHEM, null AS TABLE_CATALOG
	FROM schemas
	ORDER BY TABLE_SCHEM;

-- getCatalogs
SELECT 'default' AS TABLE_CAT;

-- getColumns
SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM,
tables.name AS TABLE_NAME, columns.name AS COLUMN_NAME,
columns.type AS TYPE_NAME, columns.type_digits AS COLUMN_SIZE,
columns.type_scale AS DECIMAL_DIGITS, 0 AS BUFFER_LENGTH,
10 AS NUM_PREC_RADIX, "null" AS nullable, null AS REMARKS,
columns.default AS COLUMN_DEF, 0 AS SQL_DATA_TYPE,
0 AS SQL_DATETIME_SUB, 0 AS CHAR_OCTET_LENGTH,
columns."number" + 1 AS ORDINAL_POSITION, null AS SCOPE_CATALOG,
null AS SCOPE_SCHEMA, null AS SCOPE_TABLE
	FROM columns, tables, schemas
	WHERE columns.table_id = tables.id
		AND tables.schema_id = schemas.id
	ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION;

-- getBestRowIdentifier
SELECT columns.name AS COLUMN_NAME, columns.type AS TYPE_NAME,
columns.type_digits AS COLUMN_SIZE, 0 AS BUFFER_LENGTH,
columns.type_scale AS DECIMAL_DIGITS, keys.type AS keytype
	FROM keys, keycolumns, tables, columns, schemas
	WHERE keys.id = keycolumns.id AND keys.table_id = tables.id
		AND keys.table_id = columns.table_id
		AND keycolumns."column" = columns.name
		AND tables.schema_id = schemas.id
		AND keys.type IN (0, 1)
	ORDER BY keytype;

-- getPrimaryKeys
SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM,
tables.name AS TABLE_NAME, keycolumns."column" AS COLUMN_NAME,
keys.type AS KEY_SEQ, keys.name AS PK_NAME
	FROM keys, keycolumns, tables, schemas
	WHERE keys.id = keycolumns.id AND keys.table_id = tables.id
		AND tables.schema_id = schemas.id
		AND keys.type = 0
	ORDER BY COLUMN_NAME;

-- getImportedKeys & getExportedKeys & getCrossReference
SELECT null AS PKTABLE_CAT, pkschema.name AS PKTABLE_SCHEM,
pktable.name AS PKTABLE_NAME, pkkeycol."column" AS PKCOLUMN_NAME,
null AS FKTABLE_CAT, fkschema.name AS FKTABLE_SCHEM,
fktable.name AS FKTABLE_NAME, fkkeycol."column" AS FKCOLUMN_NAME,
fkkey.type AS KEY_SEQ, 0 AS UPDATE_RULE,
0 AS DELETE_RULE,
fkkey.name AS FK_NAME, pkkey.name AS PK_NAME,
0 AS DEFERRABILITY
	FROM keys AS fkkey, keys AS pkkey, keycolumns AS fkkeycol,
	keycolumns AS pkkeycol, tables AS fktable, tables AS pktable,
	schemas AS fkschema, schemas AS pkschema
	WHERE fktable.id = fkkey.table_id
		AND pktable.id = pkkey.table_id
		AND	fkkey.id = fkkeycol.id
		AND pkkey.id = pkkeycol.id
		AND fkschema.id = fktable.schema_id
		AND pkschema.id = pktable.schema_id
		AND fkkey.rkey > -1
		AND fkkey.rkey = pkkey.id
	ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ;

-- getAttributes
-- returns currently one row, while I would expect 0 from the query
SELECT '' AS TYPE_CAT, '' AS TYPE_SCHEM, '' AS TYPE_NAME,
'' AS ATTR_NAME, '' AS ATTR_TYPE_NAME, 0 AS ATTR_SIZE,
0 AS DECIMAL_DIGITS, 0 AS NUM_PREC_RADIX, 0 AS NULLABLE,
'' AS REMARKS, '' AS ATTR_DEF, 0 AS SQL_DATA_TYPE,
0 AS SQL_DATETIME_SUB, 0 AS CHAR_OCTET_LENGTH,
0 AS ORDINAL_POSITION, 'YES' AS IS_NULLABLE,
'' AS SCOPE_CATALOG, '' AS SCOPE_SCHEMA, '' AS SCOPE_TABLE,
0 AS SOURCE_DATA_TYPE WHERE 1 = 0;

-- getIndexInfo
SELECT null AS TABLE_CAT, schemas.name AS TABLE_SCHEM,
tables.name AS TABLE_NAME, idxs.type as nonunique,
null AS INDEX_QUALIFIER, idxs.name AS INDEX_NAME,
0 AS TYPE,
columns."number" AS ORDINAL_POSITION, columns.name AS COLUMN_NAME,
null AS ASC_OR_DESC, 0 AS PAGES,
null AS FILTER_CONDITION
	FROM idxs, columns, keycolumns, tables, schemas
	WHERE idxs.table_id = tables.id
		AND tables.schema_id = schemas.id
		AND keycolumns.id = idxs.id
		AND columns.name = keycolumns."column"
		AND columns.table_id = tables.id
		AND idxs.name NOT IN (SELECT name FROM keys WHERE type <> 1)
	ORDER BY nonunique, TYPE, INDEX_NAME, ORDINAL_POSITION;
