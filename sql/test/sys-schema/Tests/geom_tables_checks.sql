-- Data integrity checks on geom specific tables (see geom/sql/40_geom.sql)

-- Primary Key checks
SELECT COUNT(*) AS duplicates, "srid" FROM "sys"."spatial_ref_sys" GROUP BY "srid" HAVING COUNT(*) > 1;

-- Alternate Key Uniqueness checks
SELECT COUNT(*) AS duplicates, "auth_name", "auth_srid", "srtext", "proj4text" FROM "sys"."spatial_ref_sys" GROUP BY "auth_name", "auth_srid", "srtext", "proj4text" HAVING COUNT(*) > 1;

-- Foreign Key checks
SELECT "auth_srid", * FROM "sys"."spatial_ref_sys" WHERE "auth_srid" NOT IN (SELECT "srid" FROM "sys"."spatial_ref_sys");

-- NOT NULL checks
SELECT "srid", * FROM "sys"."spatial_ref_sys" WHERE "srid" IS NULL;
SELECT "auth_name", * FROM "sys"."spatial_ref_sys" WHERE "auth_name" IS NULL;
SELECT "auth_srid", * FROM "sys"."spatial_ref_sys" WHERE "auth_srid" IS NULL;
SELECT "srtext", * FROM "sys"."spatial_ref_sys" WHERE "srtext" IS NULL;
SELECT "proj4text", * FROM "sys"."spatial_ref_sys" WHERE "proj4text" IS NULL;

-- Character string data max length violation checks (see check_MaxStrLength_violations.sql for query to generate the below queries)
SELECT '"sys"."geometry_columns"."f_geometry_column"' as full_col_nm, 1024 as max_allowed_length, length("f_geometry_column") as data_length, t."f_geometry_column" as data_value FROM "sys"."geometry_columns" t WHERE "f_geometry_column" IS NOT NULL AND length("f_geometry_column") > (select type_digits from sys._columns where name = 'f_geometry_column' and table_id in (select id from tables where name = 'geometry_columns' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."geometry_columns"."f_table_catalog"' as full_col_nm, 1 as max_allowed_length, length("f_table_catalog") as data_length, t."f_table_catalog" as data_value FROM "sys"."geometry_columns" t WHERE "f_table_catalog" IS NOT NULL AND length("f_table_catalog") > (select type_digits from sys._columns where name = 'f_table_catalog' and table_id in (select id from tables where name = 'geometry_columns' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."geometry_columns"."f_table_name"' as full_col_nm, 1024 as max_allowed_length, length("f_table_name") as data_length, t."f_table_name" as data_value FROM "sys"."geometry_columns" t WHERE "f_table_name" IS NOT NULL AND length("f_table_name") > (select type_digits from sys._columns where name = 'f_table_name' and table_id in (select id from tables where name = 'geometry_columns' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."geometry_columns"."f_table_schema"' as full_col_nm, 1024 as max_allowed_length, length("f_table_schema") as data_length, t."f_table_schema" as data_value FROM "sys"."geometry_columns" t WHERE "f_table_schema" IS NOT NULL AND length("f_table_schema") > (select type_digits from sys._columns where name = 'f_table_schema' and table_id in (select id from tables where name = 'geometry_columns' and schema_id in (select id from sys.schemas where name = 'sys')));

SELECT '"sys"."spatial_ref_sys"."auth_name"' as full_col_nm, 256 as max_allowed_length, length("auth_name") as data_length, t."auth_name" as data_value FROM "sys"."spatial_ref_sys" t WHERE "auth_name" IS NOT NULL AND length("auth_name") > (select type_digits from sys._columns where name = 'auth_name' and table_id in (select id from tables where name = 'spatial_ref_sys' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."spatial_ref_sys"."proj4text"' as full_col_nm, 2048 as max_allowed_length, length("proj4text") as data_length, t."proj4text" as data_value FROM "sys"."spatial_ref_sys" t WHERE "proj4text" IS NOT NULL AND length("proj4text") > (select type_digits from sys._columns where name = 'proj4text' and table_id in (select id from tables where name = 'spatial_ref_sys' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."spatial_ref_sys"."srtext"' as full_col_nm, 2048 as max_allowed_length, length("srtext") as data_length, t."srtext" as data_value FROM "sys"."spatial_ref_sys" t WHERE "srtext" IS NOT NULL AND length("srtext") > (select type_digits from sys._columns where name = 'srtext' and table_id in (select id from tables where name = 'spatial_ref_sys' and schema_id in (select id from sys.schemas where name = 'sys')));

