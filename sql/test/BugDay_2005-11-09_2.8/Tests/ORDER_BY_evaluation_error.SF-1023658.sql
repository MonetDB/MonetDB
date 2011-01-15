SELECT *
FROM "keys", "keycolumns", "tables", "schemas"
WHERE "keys"."id" = "keycolumns"."id"
  AND "keys"."table_id" = "tables"."id"
  AND "tables"."schema_id" = "schemas"."id"
  AND "tables"."system" = FALSE
  AND "keys"."type" = 0;

SELECT *
FROM "keys", "keycolumns", "tables", "schemas"
WHERE "keys"."id" = "keycolumns"."id"
  AND "keys"."table_id" = "tables"."id"
  AND "tables"."schema_id" = "schemas"."id"
  AND "tables"."system" = FALSE
  AND "keys"."type" = 0
ORDER BY "keycolumns"."column";
