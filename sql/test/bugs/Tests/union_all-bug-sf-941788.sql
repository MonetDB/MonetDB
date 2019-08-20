SELECT * FROM ( SELECT null AS
"TABLE_CAT", "schemas"."name" AS "TABLE_SCHEM",
"ptables"."name" AS "TABLE_NAME", 'SYSTEM TABLE' AS
"TABLE_TYPE", '' AS "REMARKS", null AS "TYPE_CAT", null
AS "TYPE_SCHEM", null AS "TYPE_NAME", 'id' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM "_tables" as ptables, "schemas" WHERE
"ptables"."schema_id" = "schemas"."id" AND
"ptables"."type" = 0 AND "ptables"."system" = true
AND "ptables"."name" IN ('args', 'columns', 'functions', 'idxs',
    'objects', 'keys', 'modules', 'sequences')
UNION ALL SELECT null AS
"TABLE_CAT", "schemas"."name" AS "TABLE_SCHEM",
"ttables"."name" AS "TABLE_NAME", 'TABLE' AS
"TABLE_TYPE", '' AS "REMARKS", null AS "TYPE_CAT", null
AS "TYPE_SCHEM", null AS "TYPE_NAME", 'id' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS
"REF_GENERATION" FROM tmp."_tables" as ttables, "schemas" WHERE
"ttables"."schema_id" = "schemas"."id"  and "ttables"."system" = true
AND "ttables"."name" IN ('args', 'columns', 'functions', 'idxs',
    'objects', 'keys', 'modules', 'sequences')
) AS "ttables" WHERE 1 = 1 
ORDER BY "TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME";

SELECT * FROM (
SELECT null AS "TABLE_CAT", "schemas"."name" AS
"TABLE_SCHEM", "ptables"."name" AS "TABLE_NAME",
'SYSTEM TABLE' AS "TABLE_TYPE", '' AS "REMARKS", null
AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'id' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS "REF_GENERATION"
FROM "_tables" as ptables, "schemas" WHERE "ptables"."schema_id" =
"schemas"."id" AND "ptables"."type" = 0 AND "ptables"."system" = true
AND "ptables"."name" IN ('args', 'columns', 'functions', 'idxs',
    'objects', 'keys', 'modules', 'sequences')
UNION ALL
SELECT null AS "TABLE_CAT", "schemas"."name" AS
"TABLE_SCHEM", "ttables"."name" AS "TABLE_NAME",
'SESSION TEMPORARY TABLE' AS "TABLE_TYPE", '' AS
"REMARKS", null AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'id' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS "REF_GENERATION"
FROM tmp."_tables" as ttables, "schemas" WHERE "ttables"."schema_id" =
"schemas"."id" AND "ttables"."type" = 0 and "ttables"."system" = true
AND "ttables"."name" IN ('args', 'columns', 'functions', 'idxs',
    'objects', 'keys', 'modules', 'sequences')
UNION ALL
SELECT null AS "TABLE_CAT", "schemas"."name" AS
"TABLE_SCHEM", "ttables"."name" AS "TABLE_NAME",
'TEMPORARY TABLE' AS "TABLE_TYPE", '' AS "REMARKS",
null AS "TYPE_CAT", null AS "TYPE_SCHEM",
null AS "TYPE_NAME", 'id' AS
"SELF_REFERENCING_COL_NAME", 'SYSTEM' AS "REF_GENERATION"
FROM tmp."_tables" as ttables, "schemas" WHERE "ttables"."schema_id" =
"schemas"."id" AND "ttables"."commit_action" > 0 
AND "ttables"."name" IN ('args', 'columns', 'functions', 'idxs',
    'objects', 'keys', 'modules', 'sequences')
) AS "ttables" WHERE 1 = 1
ORDER BY "TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME";
