create table test (
	id1 int,
	id2 int not null,
	id3 int null
);
SELECT * FROM ( SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'SYSTEM TABLE' AS "TABLE_TYPE", '' AS
"REMARKS", null AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'rowid' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM "tables", "schemas" WHERE
"tables"."schema_id" = "schemas"."id" AND
"tables"."system" = true AND "tables"."type" = 0
UNION ALL SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'TABLE' AS "TABLE_TYPE", '' AS "REMARKS",
null AS "TYPE_CAT", null AS "TYPE_SCHEM", null AS
"TYPE_NAME", 'rowid' AS "SELF_REFERENCING_COL_NAME",
'SYSTEM' AS "REF_GENERATION" FROM "tables", "schemas"
WHERE "tables"."schema_id" = "schemas"."id" AND
"tables"."system" = false AND "tables".name = 'test'
AND "tables"."type" = 0 UNION ALL SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'SYSTEM VIEW' AS "TABLE_TYPE", '' AS
"REMARKS", null AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'rowid' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM "tables", "schemas" WHERE
"tables"."schema_id" = "schemas"."id" AND
"tables"."system" = true AND "tables"."type" = 1
UNION ALL SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'VIEW' AS "TABLE_TYPE", '' AS "REMARKS",
null AS "TYPE_CAT", null AS "TYPE_SCHEM", null AS
"TYPE_NAME", 'rowid' AS "SELF_REFERENCING_COL_NAME",
'SYSTEM' AS "REF_GENERATION" FROM "tables", "schemas"
WHERE "tables"."schema_id" = "schemas"."id" AND
"tables"."system" = false AND "tables".name = 'test'
AND "tables"."type" = 1 UNION ALL SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'SYSTEM SESSION TABLE' AS "TABLE_TYPE",
'' AS "REMARKS", null AS "TYPE_CAT", null AS
"TYPE_SCHEM", null AS "TYPE_NAME", 'rowid' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM tmp."_tables" AS "tables",
"schemas" WHERE "tables"."schema_id" = "schemas"."id"
AND "tables"."system" = true AND "tables"."type" =
0 UNION ALL SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'SESSION TABLE' AS "TABLE_TYPE", '' AS
"REMARKS", null AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'rowid' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM tmp."_tables" AS "tables",
"schemas" WHERE "tables"."schema_id" = "schemas"."id"
AND "tables"."system" = false AND "tables".name = 'test'
AND "tables"."type" = 0 UNION ALL SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'SYSTEM SESSION VIEW' AS "TABLE_TYPE", ''
AS "REMARKS", null AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'rowid' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM tmp."_tables" AS "tables",
"schemas" WHERE "tables"."schema_id" = "schemas"."id"
AND "tables"."system" = true AND "tables"."type" =
1 UNION ALL SELECT 'demo' AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", 'SESSION VIEW' AS "TABLE_TYPE", '' AS
"REMARKS", null AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'rowid' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM tmp."_tables" AS "tables",
"schemas" WHERE "tables"."schema_id" = "schemas"."id"
AND "tables"."system" = false AND "tables".name = 'test' 
AND "tables"."type" = 1 ) AS "tables" WHERE 1 = 1 AND ("TABLE_TYPE" LIKE
'TABLE' OR "TABLE_TYPE" LIKE 'VIEW') ORDER BY
"TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME" ;
