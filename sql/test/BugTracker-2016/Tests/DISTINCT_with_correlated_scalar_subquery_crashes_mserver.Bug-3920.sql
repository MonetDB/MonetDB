SELECT "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT cast(NULL as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT cast('' as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast('' as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(NULL as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(NULL as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(null as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(null as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(null as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args" where "args"."func_id" = "functions"."id" and "args"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions"."language" WHEN 0 THEN "functions"."mod" || '.' || "functions"."func" ELSE "schemas"."name" || '.' || "functions"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions", "sys"."schemas" WHERE "functions"."schema_id" = "schemas"."id"
AND "functions"."type" = 2;

