CREATE USER "dbtapestry" WITH PASSWORD 'dbtapestry' NAME 'DB Tapestry' SCHEMA "sys";
CREATE SCHEMA "dbtapestry" AUTHORIZATION "dbtapestry";
ALTER USER "dbtapestry" SET SCHEMA "dbtapestry";
-- after this dbtapestry user gets "sys" schema; evil?
DROP SCHEMA "dbtapestry";
SELECT "users"."name", "schemas"."name"
	FROM "users", "schemas"
	WHERE "users"."default_schema" = "schemas"."id"
		AND "users"."name" LIKE 'dbtapestry';
