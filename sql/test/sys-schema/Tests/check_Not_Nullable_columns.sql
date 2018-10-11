-- query to fecth all columns (of all tables in all schemas) which are specified as NOT NULL
-- select s.name, t.name, c.name
--   from columns c join tables t on c.table_id = t.id join schemas s on t.schema_id = s.id
--   where c."null" = false and t.type not in (1, 11) order by s.name, t.name, c.name;

-- query used to synthesize SQLs (excluding bam schema) for checking where the NOT NULL column has a NULL value
-- select 'SELECT "'||c.name||'", * FROM "'||s.name||'"."'||t.name||'" WHERE "'||c.name||'" IS NULL;' AS qry
--   from columns c join tables t on c.table_id = t.id join schemas s on t.schema_id = s.id
--  where c."null" = false and t.type not in (1, 11) and s.name <> 'bam' order by s.name, t.name, c.name;
-- 20 rows:
-- all in sys schema
SELECT "id", * FROM "sys"."comments" WHERE "id" IS NULL;
SELECT "remark", * FROM "sys"."comments" WHERE "remark" IS NULL;

SELECT "dependency_type_id", * FROM "sys"."dependency_types" WHERE "dependency_type_id" IS NULL;
SELECT "dependency_type_name", * FROM "sys"."dependency_types" WHERE "dependency_type_name" IS NULL;

SELECT "language_id", * FROM "sys"."function_languages" WHERE "language_id" IS NULL;
SELECT "language_name", * FROM "sys"."function_languages" WHERE "language_name" IS NULL;

SELECT "function_type_id", * FROM "sys"."function_types" WHERE "function_type_id" IS NULL;
SELECT "function_type_name", * FROM "sys"."function_types" WHERE "function_type_name" IS NULL;
SELECT "function_type_keyword", * FROM "sys"."function_types" WHERE "function_type_keyword" IS NULL;

SELECT "index_type_id", * FROM "sys"."index_types" WHERE "index_type_id" IS NULL;
SELECT "index_type_name", * FROM "sys"."index_types" WHERE "index_type_name" IS NULL;

SELECT "key_type_id", * FROM "sys"."key_types" WHERE "key_type_id" IS NULL;
SELECT "key_type_name", * FROM "sys"."key_types" WHERE "key_type_name" IS NULL;

SELECT "keyword", * FROM "sys"."keywords" WHERE "keyword" IS NULL;

SELECT "privilege_code_id", * FROM "sys"."privilege_codes" WHERE "privilege_code_id" IS NULL;
SELECT "privilege_code_name", * FROM "sys"."privilege_codes" WHERE "privilege_code_name" IS NULL;

-- moved to: geom_tables_checks.sql
-- SELECT "srid", * FROM "sys"."spatial_ref_sys" WHERE "srid" IS NULL;

SELECT "function_id", * FROM "sys"."systemfunctions" WHERE "function_id" IS NULL;

SELECT "table_type_id", * FROM "sys"."table_types" WHERE "table_type_id" IS NULL;
SELECT "table_type_name", * FROM "sys"."table_types" WHERE "table_type_name" IS NULL;


-- However many columns -which shouldn't have nulls- are not created as NOT NULL in the SQL data dictionary!
-- identify those columns (by generating their statistics and quering the statistics table on nils = 0 and no 'nil' as min or max value)
-- queries used to synthesize SQLs for checking where the NOT NULL column has a NULL value
-- analyze sys;
-- analyze sys.statistics;
-- select 'SELECT "'||c.name||'", * FROM "'||s.name||'"."'||t.name||'" WHERE "'||c.name||'" IS NULL;' AS qry
--   from statistics st join columns c on st.column_id = c.id join tables t on c.table_id = t.id join schemas s on t.schema_id = s.id
--  where st.nils = 0 and st.minval <> 'nil' and st.maxval <> 'nil'
--    and "null" <> false and t.type not in (1, 11) and s.name <> 'bam' order by s.name, t.name, c.number, c.name;
-- 96 rows:
-- all in sys schema
SELECT "id", * FROM "sys"."_columns" WHERE "id" IS NULL;
SELECT "name", * FROM "sys"."_columns" WHERE "name" IS NULL;
SELECT "type", * FROM "sys"."_columns" WHERE "type" IS NULL;
SELECT "type_digits", * FROM "sys"."_columns" WHERE "type_digits" IS NULL;
SELECT "type_scale", * FROM "sys"."_columns" WHERE "type_scale" IS NULL;
SELECT "table_id", * FROM "sys"."_columns" WHERE "table_id" IS NULL;
SELECT "null", * FROM "sys"."_columns" WHERE "null" IS NULL;
SELECT "number", * FROM "sys"."_columns" WHERE "number" IS NULL;

SELECT "id", * FROM "sys"."_tables" WHERE "id" IS NULL;
SELECT "name", * FROM "sys"."_tables" WHERE "name" IS NULL;
SELECT "schema_id", * FROM "sys"."_tables" WHERE "schema_id" IS NULL;
SELECT "type", * FROM "sys"."_tables" WHERE "type" IS NULL;
SELECT "system", * FROM "sys"."_tables" WHERE "system" IS NULL;
SELECT "commit_action", * FROM "sys"."_tables" WHERE "commit_action" IS NULL;
SELECT "access", * FROM "sys"."_tables" WHERE "access" IS NULL;

SELECT "id", * FROM "sys"."args" WHERE "id" IS NULL;
SELECT "func_id", * FROM "sys"."args" WHERE "func_id" IS NULL;
SELECT "name", * FROM "sys"."args" WHERE "name" IS NULL;
SELECT "type", * FROM "sys"."args" WHERE "type" IS NULL;
SELECT "type_digits", * FROM "sys"."args" WHERE "type_digits" IS NULL;
SELECT "type_scale", * FROM "sys"."args" WHERE "type_scale" IS NULL;
SELECT "inout", * FROM "sys"."args" WHERE "inout" IS NULL;
SELECT "number", * FROM "sys"."args" WHERE "number" IS NULL;

SELECT "id", * FROM "sys"."auths" WHERE "id" IS NULL;
SELECT "name", * FROM "sys"."auths" WHERE "name" IS NULL;
SELECT "grantor", * FROM "sys"."auths" WHERE "grantor" IS NULL;

SELECT "name", * FROM "sys"."db_user_info" WHERE "name" IS NULL;
SELECT "fullname", * FROM "sys"."db_user_info" WHERE "fullname" IS NULL;
SELECT "default_schema", * FROM "sys"."db_user_info" WHERE "default_schema" IS NULL;

SELECT "id", * FROM "sys"."dependencies" WHERE "id" IS NULL;
SELECT "depend_id", * FROM "sys"."dependencies" WHERE "depend_id" IS NULL;
SELECT "depend_type", * FROM "sys"."dependencies" WHERE "depend_type" IS NULL;

SELECT "id", * FROM "sys"."functions" WHERE "id" IS NULL;
SELECT "name", * FROM "sys"."functions" WHERE "name" IS NULL;
SELECT "func", * FROM "sys"."functions" WHERE "func" IS NULL;
SELECT "mod", * FROM "sys"."functions" WHERE "mod" IS NULL;
SELECT "language", * FROM "sys"."functions" WHERE "language" IS NULL;
SELECT "type", * FROM "sys"."functions" WHERE "type" IS NULL;
SELECT "side_effect", * FROM "sys"."functions" WHERE "side_effect" IS NULL;
SELECT "varres", * FROM "sys"."functions" WHERE "varres" IS NULL;
SELECT "vararg", * FROM "sys"."functions" WHERE "vararg" IS NULL;
SELECT "schema_id", * FROM "sys"."functions" WHERE "schema_id" IS NULL;
SELECT "system", * FROM "sys"."functions" WHERE "system" IS NULL;

SELECT "id", * FROM "sys"."idxs" WHERE "id" IS NULL;
SELECT "table_id", * FROM "sys"."idxs" WHERE "table_id" IS NULL;
SELECT "type", * FROM "sys"."idxs" WHERE "type" IS NULL;
SELECT "name", * FROM "sys"."idxs" WHERE "name" IS NULL;

SELECT "id", * FROM "sys"."keys" WHERE "id" IS NULL;
SELECT "table_id", * FROM "sys"."keys" WHERE "table_id" IS NULL;
SELECT "type", * FROM "sys"."keys" WHERE "type" IS NULL;
SELECT "name", * FROM "sys"."keys" WHERE "name" IS NULL;
SELECT "rkey", * FROM "sys"."keys" WHERE "rkey" IS NULL;
SELECT "action", * FROM "sys"."keys" WHERE "action" IS NULL;

SELECT "id", * FROM "sys"."objects" WHERE "id" IS NULL;
SELECT "name", * FROM "sys"."objects" WHERE "name" IS NULL;
SELECT "nr", * FROM "sys"."objects" WHERE "nr" IS NULL;

SELECT "obj_id", * FROM "sys"."privileges" WHERE "obj_id" IS NULL;
SELECT "auth_id", * FROM "sys"."privileges" WHERE "auth_id" IS NULL;
SELECT "privileges", * FROM "sys"."privileges" WHERE "privileges" IS NULL;
SELECT "grantor", * FROM "sys"."privileges" WHERE "grantor" IS NULL;
SELECT "grantable", * FROM "sys"."privileges" WHERE "grantable" IS NULL;

SELECT "id", * FROM "sys"."schemas" WHERE "id" IS NULL;
SELECT "name", * FROM "sys"."schemas" WHERE "name" IS NULL;
SELECT "authorization", * FROM "sys"."schemas" WHERE "authorization" IS NULL;
SELECT "owner", * FROM "sys"."schemas" WHERE "owner" IS NULL;
SELECT "system", * FROM "sys"."schemas" WHERE "system" IS NULL;

-- moved to: geom_tables_checks.sql
-- SELECT "auth_name", * FROM "sys"."spatial_ref_sys" WHERE "auth_name" IS NULL;
-- SELECT "auth_srid", * FROM "sys"."spatial_ref_sys" WHERE "auth_srid" IS NULL;
-- SELECT "srtext", * FROM "sys"."spatial_ref_sys" WHERE "srtext" IS NULL;
-- SELECT "proj4text", * FROM "sys"."spatial_ref_sys" WHERE "proj4text" IS NULL;

SELECT "column_id", * FROM "sys"."statistics" WHERE "column_id" IS NULL;
SELECT "type", * FROM "sys"."statistics" WHERE "type" IS NULL;
SELECT "width", * FROM "sys"."statistics" WHERE "width" IS NULL;
SELECT "stamp", * FROM "sys"."statistics" WHERE "stamp" IS NULL;
SELECT "sample", * FROM "sys"."statistics" WHERE "sample" IS NULL;
SELECT "count", * FROM "sys"."statistics" WHERE "count" IS NULL;
SELECT "unique", * FROM "sys"."statistics" WHERE "unique" IS NULL;
SELECT "nils", * FROM "sys"."statistics" WHERE "nils" IS NULL;
SELECT "minval", * FROM "sys"."statistics" WHERE "minval" IS NULL;
SELECT "maxval", * FROM "sys"."statistics" WHERE "maxval" IS NULL;
SELECT "sorted", * FROM "sys"."statistics" WHERE "sorted" IS NULL;
SELECT "revsorted", * FROM "sys"."statistics" WHERE "revsorted" IS NULL;

SELECT "id", * FROM "sys"."triggers" WHERE "id" IS NULL;
SELECT "name", * FROM "sys"."triggers" WHERE "name" IS NULL;
SELECT "table_id", * FROM "sys"."triggers" WHERE "table_id" IS NULL;
SELECT "time", * FROM "sys"."triggers" WHERE "time" IS NULL;
SELECT "orientation", * FROM "sys"."triggers" WHERE "orientation" IS NULL;
SELECT "event", * FROM "sys"."triggers" WHERE "event" IS NULL;
SELECT "statement", * FROM "sys"."triggers" WHERE "statement" IS NULL;

SELECT "id", * FROM "sys"."types" WHERE "id" IS NULL;
SELECT "systemname", * FROM "sys"."types" WHERE "systemname" IS NULL;
SELECT "sqlname", * FROM "sys"."types" WHERE "sqlname" IS NULL;
SELECT "digits", * FROM "sys"."types" WHERE "digits" IS NULL;
SELECT "scale", * FROM "sys"."types" WHERE "scale" IS NULL;
SELECT "radix", * FROM "sys"."types" WHERE "radix" IS NULL;
SELECT "eclass", * FROM "sys"."types" WHERE "eclass" IS NULL;
SELECT "schema_id", * FROM "sys"."types" WHERE "schema_id" IS NULL;

