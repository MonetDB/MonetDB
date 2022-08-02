SELECT * FROM "sys"."keys" AS "keys", "sys"."objects" AS
"objects", "sys"."tables" AS "tables",
"sys"."schemas" AS "schemas" WHERE "keys"."id" =
"objects"."id" AND "keys"."table_id" = "tables"."id"
AND "tables"."schema_id" = "schemas"."id" AND
"keys"."type" = 0 AND "schemas"."name" LIKE 'sys' AND
"tables"."name" LIKE 'x';

SELECT * FROM "sys"."keys" AS "keys", "sys"."objects" AS
"objects", "sys"."tables" AS "tables",
"sys"."schemas" AS "schemas" WHERE "keys"."id" =
"objects"."id" AND "keys"."table_id" = "tables"."id"
AND "tables"."schema_id" = "schemas"."id" AND
"keys"."type" = 0 AND "schemas"."name" LIKE 'sys' AND
"tables"."name" LIKE 'x' ORDER BY "objects"."name";

SELECT cast(null AS varchar(1)) AS "TABLE_CAT",
"schemas"."name" AS "TABLE_SCHEM", "tables"."name" AS
"TABLE_NAME", "objects"."name" AS "COLUMN_NAME",
"keys"."type" AS "KEY_SEQ", "keys"."name" AS "PK_NAME"
FROM "sys"."keys" AS "keys", "sys"."objects" AS
"objects", "sys"."tables" AS "tables",
"sys"."schemas" AS "schemas" WHERE "keys"."id" =
"objects"."id" AND "keys"."table_id" = "tables"."id"
AND "tables"."schema_id" = "schemas"."id" AND
"keys"."type" = 0 AND "schemas"."name" LIKE 'sys' AND
"tables"."name" LIKE 'x' ORDER BY "COLUMN_NAME";
