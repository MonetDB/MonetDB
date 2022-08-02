set schema sys;

SELECT cast(CASE WHEN "columns"."default" IS NOT NULL AND "columns"."default" LIKE 'next value for %' THEN 'YES' ELSE 'NO' END AS varchar(3)) AS "IS_AUTOINCREMENT"
FROM "sys"."columns"
WHERE "columns"."default" LIKE 'next value for %';

set schema tmp;

SELECT cast(CASE WHEN "columns"."default" IS NOT NULL AND "columns"."default" LIKE 'next value for %' THEN 'YES' ELSE 'NO' END AS varchar(3)) AS "IS_AUTOINCREMENT"
FROM "sys"."columns"
WHERE "columns"."default" LIKE 'next value for %';

set schema profiler;

SELECT cast(CASE WHEN "columns"."default" IS NOT NULL AND "columns"."default" LIKE 'next value for %' THEN 'YES' ELSE 'NO' END AS varchar(3)) AS "IS_AUTOINCREMENT"
FROM "sys"."columns"
WHERE "columns"."default" LIKE 'next value for %';

