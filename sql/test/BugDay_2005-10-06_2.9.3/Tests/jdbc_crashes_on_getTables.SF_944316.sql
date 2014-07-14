SELECT * FROM (
	SELECT
		null AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM TABLE' AS "TABLE_TYPE",
		'' AS "REMARKS", null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'id' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id" AND "tables"."type" = 0
)
AS "tables"
WHERE 1 = 1 AND ("TABLE_TYPE" LIKE 'TABLE' OR "TABLE_TYPE" LIKE 'VIEW')
ORDER BY "TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME";
