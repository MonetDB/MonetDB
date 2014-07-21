SELECT "f"."id",
       "f"."name",
       "f"."mod",
       "f"."func",
       "a"."name",
       "a"."type",
       "a"."type_digits",
       "a"."type_scale",
       "a"."number"
FROM "sys"."args" "a",
     "sys"."functions" "f"
WHERE "f"."id" NOT IN (SELECT "function_id" FROM "sys"."systemfunctions") AND
      "f"."language" <> 1 AND
      "a"."func_id" = "f"."id"
ORDER BY "f"."id", "a"."number";
