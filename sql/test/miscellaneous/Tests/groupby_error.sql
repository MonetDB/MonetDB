CREATE SCHEMA "kagami_dump";
CREATE TABLE "kagami_dump"."test_task" ("sys_id" CHAR(32) DEFAULT '', "number" VARCHAR(40), "parent" VARCHAR(32));
INSERT INTO "kagami_dump".test_task(sys_id, number, parent) VALUES ('aaa', 'T0001', null),('bbb','T0002','aaa');

SELECT parent."sys_id" FROM "kagami_dump"."test_task" parent INNER JOIN "kagami_dump"."test_task" child ON child."parent" = parent."sys_id" GROUP BY parent."sys_id" HAVING count(child."sys_id") >= 1 ORDER BY parent."number"; --error, parent."number" requires an aggregate function

DROP SCHEMA "kagami_dump" CASCADE;
