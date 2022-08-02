-- a little exploration...
-- all queries below run, but produce lots of scary messages on the
-- Mserver terminal.  The last query seems just to be a little bit
-- too much and crashes the server.
-- Although the first queries run, they do not result in the desired
-- output, but return an error instead.

-- a very small version of the query, produces lots of terminal bogus
SELECT 1 AS number;
SELECT * FROM (
 	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM TABLE' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 0
) AS "tables"
WHERE 1 = 1
	AND ("TABLE_TYPE" LIKE 'TABLE' OR "TABLE_TYPE" LIKE 'VIEW')
ORDER BY "TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME";


-- the full query but with two unions less
SELECT 2 AS number;
SELECT * FROM (
-- 	SELECT 'demo' AS "TABLE_CAT",
-- 		"schemas"."name" AS "TABLE_SCHEM",
-- 		"tables"."name" AS "TABLE_NAME",
-- 		'SYSTEM TABLE' AS "TABLE_TYPE",
-- 		'' AS "REMARKS",
-- 		null AS "TYPE_CAT",
-- 		null AS "TYPE_SCHEM",
-- 		null AS "TYPE_NAME",
-- 		'rowid' AS "SELF_REFERENCING_COL_NAME",
-- 		'SYSTEM' AS "REF_GENERATION"
-- 	FROM "tables", "schemas"
-- 	WHERE "tables"."schema_id" = "schemas"."id"
-- 		AND "tables"."system" = true
-- 		AND "tables"."type" = 0
--
-- 	UNION ALL

-- 	SELECT 'demo' AS "TABLE_CAT",
-- 		"schemas"."name" AS "TABLE_SCHEM",
-- 		"tables"."name" AS "TABLE_NAME",
-- 		'TABLE' AS "TABLE_TYPE",
-- 		'' AS "REMARKS",
-- 		null AS "TYPE_CAT",
-- 		null AS "TYPE_SCHEM",
-- 		null AS "TYPE_NAME",
-- 		'rowid' AS "SELF_REFERENCING_COL_NAME",
-- 		'SYSTEM' AS "REF_GENERATION"
-- 	FROM "tables", "schemas"
-- 	WHERE "tables"."schema_id" = "schemas"."id"
-- 		AND "tables"."system" = false
-- 		AND "tables"."type" = 0
--
-- 	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 1

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = false
		AND "tables"."type" = 1

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM SESSION TABLE' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 0

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SESSION TABLE' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = false
		AND "tables"."type" = 0

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM SESSION VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 1

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SESSION VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = false
		AND "tables"."type" = 1

) AS "tables"
	WHERE 1 = 1
		AND ("TABLE_TYPE" LIKE 'TABLE' OR "TABLE_TYPE" LIKE 'VIEW')
	ORDER BY "TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME";


-- the full query; which produces a crash
SELECT 3 AS number;
SELECT * FROM (
	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM TABLE' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 0

	UNION ALL

 	SELECT 'demo' AS "TABLE_CAT",
 		"schemas"."name" AS "TABLE_SCHEM",
 		"tables"."name" AS "TABLE_NAME",
 		'TABLE' AS "TABLE_TYPE",
 		'' AS "REMARKS",
 		null AS "TYPE_CAT",
 		null AS "TYPE_SCHEM",
 		null AS "TYPE_NAME",
 		'rowid' AS "SELF_REFERENCING_COL_NAME",
 		'SYSTEM' AS "REF_GENERATION"
 	FROM "tables", "schemas"
 	WHERE "tables"."schema_id" = "schemas"."id"
 		AND "tables"."system" = false
 		AND "tables"."type" = 0

 	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 1

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = false
		AND "tables"."type" = 1

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM SESSION TABLE' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 0

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SESSION TABLE' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = false
		AND "tables"."type" = 0

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SYSTEM SESSION VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = true
		AND "tables"."type" = 1

	UNION ALL

	SELECT 'demo' AS "TABLE_CAT",
		"schemas"."name" AS "TABLE_SCHEM",
		"tables"."name" AS "TABLE_NAME",
		'SESSION VIEW' AS "TABLE_TYPE",
		'' AS "REMARKS",
		null AS "TYPE_CAT",
		null AS "TYPE_SCHEM",
		null AS "TYPE_NAME",
		'rowid' AS "SELF_REFERENCING_COL_NAME",
		'SYSTEM' AS "REF_GENERATION"
	FROM tmp."_tables" AS "tables", "schemas"
	WHERE "tables"."schema_id" = "schemas"."id"
		AND "tables"."system" = false
		AND "tables"."type" = 1

) AS "tables"
	WHERE 1 = 1
		AND ("TABLE_TYPE" LIKE 'TABLE' OR "TABLE_TYPE" LIKE 'VIEW')
	ORDER BY "TABLE_TYPE", "TABLE_SCHEM", "TABLE_NAME";
