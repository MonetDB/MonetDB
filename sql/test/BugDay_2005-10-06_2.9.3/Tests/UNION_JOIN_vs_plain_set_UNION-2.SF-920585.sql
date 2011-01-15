START TRANSACTION;	-- don't feel like cleaning this whole mess up at the end

CREATE SCHEMA "t920585";

SET SCHEMA "t920585";

CREATE TABLE "_tables" (
        "id"            int     ,
        "name"          varchar (1024),
        "schema_id"     int     ,
        "query"         varchar (2048),
        "istable"       boolean ,
        "system"        boolean ,
        "commit_action" smallint
);

INSERT INTO "_tables" VALUES (4458, 'modules', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4462, 'schemas', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4466, 'types', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4475, 'functions', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4482, 'args', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4490, '_tables', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4498, '_columns', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4508, 'keys', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4514, 'idxs', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4519, 'triggers', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4530, 'keycolumns', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4536, '_tables', 4535, NULL, true, true, 2);
INSERT INTO "_tables" VALUES (4544, '_columns', 4535, NULL, true, true, 2);
INSERT INTO "_tables" VALUES (4554, 'keys', 4535, NULL, true, true, 2);
INSERT INTO "_tables" VALUES (4560, 'idxs', 4535, NULL, true, true, 2);
INSERT INTO "_tables" VALUES (4565, 'triggers', 4535, NULL, true, true, 2);
INSERT INTO "_tables" VALUES (4576, 'keycolumns', 4535, NULL, true, true, 2);
INSERT INTO "_tables" VALUES (4581, 'tables', 4457, 'SELECT * FROM (SELECT p.*, 0 AS "temporary" FROM "sys"."_tables" AS p UNION ALL SELECT t.*, 1 AS "temporary" FROM "tmp"."_tables" AS t) AS tables;', false, true, 0);
INSERT INTO "_tables" VALUES (4590, 'columns', 4457, 'SELECT * FROM (SELECT p.* FROM "sys"."_columns" AS p UNION ALL SELECT t.* FROM "tmp"."_columns" AS t) AS columns;', false, true, 0);
INSERT INTO "_tables" VALUES (4600, 'sequences', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4611, 'env', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4614, 'db_user_info', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4618, 'db_users', 4457, ';CREATE VIEW "db_users" ([system BAT]) AS BATS', false, true, 0);
INSERT INTO "_tables" VALUES (4621, 'db_scens', 4457, ';CREATE VIEW "db_scens" ([system BAT]) AS BATS', false, true, 0);
INSERT INTO "_tables" VALUES (4624, 'users', 4457, 'SELECT u."t" AS "name", ui."fullname", ui."default_schema" FROM "sys"."db_users" AS u, "sys"."db_user_info" AS ui WHERE u."h" = ui."id" AND u."h" NOT IN (SELECT s."h" FROM "sys"."db_scens" AS s WHERE s."t" NOT LIKE \'sql\');', false, true, 0);
INSERT INTO "_tables" VALUES (4628, 'user_role', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4631, 'auths', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4635, 'privileges', 4457, NULL, true, true, 0);
INSERT INTO "_tables" VALUES (4641, 'profile', 4535, NULL, true, true, 2);
INSERT INTO "_tables" VALUES (4649, 'sessions', 4535, NULL, true, true, 2);

CREATE TABLE "ttables" AS SELECT * FROM "_tables" WITH NO DATA;

CREATE TABLE "schemas" (
        "id"            int    ,
        "name"          varchar(1024),
        "authorization" int
);

INSERT INTO "schemas" VALUES (4457, 'sys', 2);
INSERT INTO "schemas" VALUES (4535, 'tmp', 2);

CREATE VIEW "tables" AS SELECT * FROM (SELECT p.*, 0 AS "temporary" FROM "t920585"."_tables" AS p UNION ALL SELECT t.*, 1 AS "temporary" FROM "t920585"."ttables" AS t) AS tables;

select name, query, istable, system, commit_action, "temporary"
	from (
		select *
			from tables
			where tables.system = 1
		union
		select *
			from tables
			where tables.system = 1
		) as a
	order by name;

select * from (
SELECT 'null' AS TABLE_CAT,
	schemas.name AS TABLE_SCHEM,
	tables.name AS TABLE_NAME,
	'SYSTEM TABLE' AS TABLE_TYPE,
	'' AS REMARKS,
	'null' AS TYPE_CAT,
	'null' AS TYPE_SCHEM,
	'null' AS TYPE_NAME,
	'id' AS SELF_REFERENCING_COL_NAME,
	'SYSTEM' AS REF_GENERATION
FROM tables, schemas
WHERE tables.schema_id = schemas.id
	AND tables.system = 1
	AND tables.istable = 0
UNION ALL
SELECT 'null' AS TABLE_CAT,
	schemas.name AS TABLE_SCHEM,
	tables.name AS TABLE_NAME,
	'TABLE' AS TABLE_TYPE,
	'' AS REMARKS,
	'null' AS TYPE_CAT,
	'null' AS TYPE_SCHEM,
	'null' AS TYPE_NAME,
	'id' AS SELF_REFERENCING_COL_NAME,
	'SYSTEM' AS REF_GENERATION
FROM tables, schemas
WHERE tables.schema_id = schemas.id
	AND tables.system = 0
	AND tables.istable = 0
	AND tables.name = 'test'
UNION ALL
SELECT 'null' AS TABLE_CAT,
	schemas.name AS TABLE_SCHEM,
	tables.name AS TABLE_NAME,
	'VIEW' AS TABLE_TYPE,
	tables.query AS REMARKS,
	'null' AS TYPE_CAT,
	'null' AS TYPE_SCHEM,
	'null' AS TYPE_NAME,
	'id' AS SELF_REFERENCING_COL_NAME,
	'SYSTEM' AS REF_GENERATION
FROM tables, schemas
WHERE tables.schema_id = schemas.id
	AND tables.istable = 0
	AND (tables.name = '_tables' or tables.name = '_columns' or tables.name = 'users') 
) as tables order by TABLE_NAME;
