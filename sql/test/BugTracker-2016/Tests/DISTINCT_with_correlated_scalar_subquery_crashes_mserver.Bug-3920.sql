CREATE TABLE sys."functions_cpy" AS SELECT * FROM sys.functions WHERE name LIKE 's%' AND schema_id IN (2000, 0) WITH DATA;
SELECT COUNT(*) FROM sys."functions_cpy";
CREATE TABLE sys."args_cpy" AS SELECT id, func_id, number FROM sys.args WHERE func_id IN (SELECT id FROM sys.functions_cpy) WITH DATA;
SELECT COUNT(*) FROM sys."args_cpy";

SELECT "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT cast(NULL as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT cast('' as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast('' as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(NULL as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, CAST((SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS smallint) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(NULL as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(null as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
AND "schemas"."name" = 'sys'
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(null as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2
ORDER BY "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME";

SELECT DISTINCT cast(null as char(1)) AS "PROCEDURE_CAT", "schemas"."name" AS "PROCEDURE_SCHEM", "functions_cpy"."name" AS "PROCEDURE_NAME"
, (SELECT COUNT(*) FROM "sys"."args_cpy" where "args_cpy"."func_id" = "functions_cpy"."id" and "args_cpy"."number" = 0) AS "PROCEDURE_TYPE"
, CAST(CASE "functions_cpy"."language" WHEN 0 THEN "functions_cpy"."mod" || '.' || "functions_cpy"."func" ELSE "schemas"."name" || '.' || "functions_cpy"."name" END AS VARCHAR(1500)) AS "SPECIFIC_NAME"
FROM "sys"."functions_cpy", "sys"."schemas" WHERE "functions_cpy"."schema_id" = "schemas"."id"
AND "functions_cpy"."type" = 2;

DROP TABLE sys.functions_cpy;
DROP TABLE sys.args_cpy;

