select name, query, istable, system, commit_action, "temporary" from (select * from tables where tables.system = 1 union select * from tables where tables.system = 1) as a;

SELECT 'null' AS TABLE_CAT,
	schemas.name AS TABLE_SCHEM,
	tables.name AS TABLE_NAME,
	'SYSTEM TABLE' AS TABLE_TYPE,
	'' AS REMARKS,
	'null' AS TYPE_CAT,
	'null' AS TYPE_SCHEM,
	'null' AS TYPE_NAME,
	'id' AS SELF_REFERENCING_COL_NAME,
	'SYSTEM' AS REF_GENERATION
FROM tables, schemas
WHERE tables.schema_id = schemas.id
	AND tables.system = 1
	AND tables.istable = 0
UNION ALL
SELECT 'null' AS TABLE_CAT,
	schemas.name AS TABLE_SCHEM,
	tables.name AS TABLE_NAME,
	'TABLE' AS TABLE_TYPE,
	'' AS REMARKS,
	'null' AS TYPE_CAT,
	'null' AS TYPE_SCHEM,
	'null' AS TYPE_NAME,
	'id' AS SELF_REFERENCING_COL_NAME,
	'SYSTEM' AS REF_GENERATION
FROM tables, schemas
WHERE tables.schema_id = schemas.id
	AND tables.system = 0
	AND tables.istable = 0
	AND tables.name = 'test'
UNION ALL
SELECT 'null' AS TABLE_CAT,
	schemas.name AS TABLE_SCHEM,
	tables.name AS TABLE_NAME,
	'VIEW' AS TABLE_TYPE,
	tables.query AS REMARKS,
	'null' AS TYPE_CAT,
	'null' AS TYPE_SCHEM,
	'null' AS TYPE_NAME,
	'id' AS SELF_REFERENCING_COL_NAME,
	'SYSTEM' AS REF_GENERATION
FROM tables, schemas
WHERE tables.schema_id = schemas.id
	AND tables.istable = 0
	AND (tables.name = 'ttables' or tables.name = 'tcolumns' or tables.name = 'users');
