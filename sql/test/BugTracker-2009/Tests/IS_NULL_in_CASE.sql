CREATE TABLE "sometable" (
"somecol" INTEGER
)
;
INSERT INTO "sometable" VALUES ( 0);
INSERT INTO "sometable" VALUES ( 1);
INSERT INTO sometable VALUES (NULL);

SELECT somecol
, CASE
WHEN somecol < 6 THEN 'small'
WHEN somecol = 6 THEN 'six'
WHEN somecol > 6 AND somecol < 9 THEN '78'
WHEN somecol IS NULL THEN 'NULL FOUND'
ELSE 'big'
END AS "NewColumnName"
FROM sometable
;

SELECT somecol
, CASE
WHEN somecol IS NULL THEN 'NULL FOUND'
WHEN somecol < 6 THEN 'small'
WHEN somecol = 6 THEN 'six'
WHEN somecol > 6 AND somecol < 9 THEN '78'
ELSE 'big'
END AS "NewColumnName"
FROM sometable
;

drop table sometable;
