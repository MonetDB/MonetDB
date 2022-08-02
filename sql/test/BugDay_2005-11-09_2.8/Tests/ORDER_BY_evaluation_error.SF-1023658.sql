SELECT *
FROM "keys", "objects", "tables", "schemas"
WHERE "keys"."id" = "objects"."id"
  AND "keys"."table_id" = "tables"."id"
  AND "tables"."schema_id" = "schemas"."id"
  AND "tables"."system" = FALSE
  AND "keys"."type" = 0;

SELECT *
FROM "keys", "objects", "tables", "schemas"
WHERE "keys"."id" = "objects"."id"
  AND "keys"."table_id" = "tables"."id"
  AND "tables"."schema_id" = "schemas"."id"
  AND "tables"."system" = FALSE
  AND "keys"."type" = 0
ORDER BY "objects"."name";
