CREATE SCHEMA "dropme";
CREATE LOCAL TEMP TABLE "dropme"."temp1" ("id" int, "name" VARCHAR(99)); --error
CREATE GLOBAL TEMP TABLE "dropme"."temp2" ("id" int, "name" VARCHAR(99)); --error
DROP SCHEMA "dropme";
START TRANSACTION;
CREATE LOCAL TEMP TABLE "temp1" ("id" int, "name" VARCHAR(99));
CREATE GLOBAL TEMP TABLE "temp2" ("id" int, "name" VARCHAR(99));
ROLLBACK;
