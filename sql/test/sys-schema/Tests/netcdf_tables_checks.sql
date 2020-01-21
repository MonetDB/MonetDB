-- Data integrity checks on netcdf specific tables  (see /sql/backends/monet5/vaults/netcdf/74_netcdf.sql)

-- Primary Key checks
SELECT COUNT(*) AS duplicates, "file_id" FROM "sys"."netcdf_files" GROUP BY "file_id" HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, "dim_id", "file_id" FROM "sys"."netcdf_dims" GROUP BY "dim_id", "file_id" HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, "var_id", "file_id" FROM "sys"."netcdf_vars" GROUP BY "var_id", "file_id" HAVING COUNT(*) > 1;
SELECT COUNT(*) AS duplicates, "var_id", "dim_id", "file_id" FROM "sys"."netcdf_vardim" GROUP BY "var_id", "dim_id", "file_id" HAVING COUNT(*) > 1;

-- Alternate Key Uniqueness checks
SELECT COUNT(*) AS duplicates, "location" FROM "sys"."netcdf_files" GROUP BY "location" HAVING COUNT(*) > 1;

-- Foreign Key checks
SELECT "file_id", * FROM "sys"."netcdf_attrs" WHERE "file_id" NOT IN (SELECT "file_id" FROM "sys"."netcdf_files");
SELECT "file_id", * FROM "sys"."netcdf_dims" WHERE "file_id" NOT IN (SELECT "file_id" FROM "sys"."netcdf_files");
SELECT "file_id", * FROM "sys"."netcdf_vars" WHERE "file_id" NOT IN (SELECT "file_id" FROM "sys"."netcdf_files");
SELECT "file_id", * FROM "sys"."netcdf_vardim" WHERE "file_id" NOT IN (SELECT "file_id" FROM "sys"."netcdf_files");
SELECT "dim_id", * FROM "sys"."netcdf_vardim" WHERE "dim_id" NOT IN (SELECT "dim_id" FROM "sys"."netcdf_dims");
SELECT "dim_id", "file_id", * FROM "sys"."netcdf_vardim" WHERE ("dim_id", "file_id") NOT IN (SELECT "dim_id", "file_id" FROM "sys"."netcdf_dims");
SELECT "var_id", * FROM "sys"."netcdf_vardim" WHERE "var_id" NOT IN (SELECT "var_id" FROM "sys"."netcdf_vars");
SELECT "var_id", "file_id", * FROM "sys"."netcdf_vardim" WHERE ("var_id", "file_id") NOT IN (SELECT "var_id", "file_id" FROM "sys"."netcdf_vars");

-- NOT NULL checks
SELECT "file_id", * FROM "sys"."netcdf_files" WHERE "file_id" IS NULL;
SELECT "location", * FROM "sys"."netcdf_files" WHERE "location" IS NULL;
SELECT "dim_id", * FROM "sys"."netcdf_dims" WHERE "dim_id" IS NULL;
SELECT "file_id", * FROM "sys"."netcdf_dims" WHERE "file_id" IS NULL;
SELECT "name", * FROM "sys"."netcdf_dims" WHERE "name" IS NULL;
SELECT "length", * FROM "sys"."netcdf_dims" WHERE "length" IS NULL;
SELECT "var_id", * FROM "sys"."netcdf_vars" WHERE "var_id" IS NULL;
SELECT "file_id", * FROM "sys"."netcdf_vars" WHERE "file_id" IS NULL;
SELECT "name", * FROM "sys"."netcdf_vars" WHERE "name" IS NULL;
SELECT "vartype", * FROM "sys"."netcdf_vars" WHERE "vartype" IS NULL;
SELECT "var_id", * FROM "sys"."netcdf_vardim" WHERE "var_id" IS NULL;
SELECT "dim_id", * FROM "sys"."netcdf_vardim" WHERE "dim_id" IS NULL;
SELECT "file_id", * FROM "sys"."netcdf_vardim" WHERE "file_id" IS NULL;
SELECT "dimpos", * FROM "sys"."netcdf_vardim" WHERE "dimpos" IS NULL;
SELECT "obj_name", * FROM "sys"."netcdf_attrs" WHERE "obj_name" IS NULL;
SELECT "att_name", * FROM "sys"."netcdf_attrs" WHERE "att_name" IS NULL;
SELECT "att_type", * FROM "sys"."netcdf_attrs" WHERE "att_type" IS NULL;
SELECT "value", * FROM "sys"."netcdf_attrs" WHERE "value" IS NULL;
SELECT "file_id", * FROM "sys"."netcdf_attrs" WHERE "file_id" IS NULL;
SELECT "gr_name", * FROM "sys"."netcdf_attrs" WHERE "gr_name" IS NULL;

-- Character string data max length violation checks (see check_MaxStrLength_violations.sql for query to generate the below queries)
SELECT '"sys"."netcdf_attrs"."att_name"' as full_col_nm, 256 as max_allowed_length, length("att_name") as data_length, t."att_name" as data_value FROM "sys"."netcdf_attrs" t WHERE "att_name" IS NOT NULL AND length("att_name") > (select type_digits from sys._columns where name = 'att_name' and table_id in (select id from tables where name = 'netcdf_attrs' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."netcdf_attrs"."att_type"' as full_col_nm, 64 as max_allowed_length, length("att_type") as data_length, t."att_type" as data_value FROM "sys"."netcdf_attrs" t WHERE "att_type" IS NOT NULL AND length("att_type") > (select type_digits from sys._columns where name = 'att_type' and table_id in (select id from tables where name = 'netcdf_attrs' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."netcdf_attrs"."gr_name"' as full_col_nm, 256 as max_allowed_length, length("gr_name") as data_length, t."gr_name" as data_value FROM "sys"."netcdf_attrs" t WHERE "gr_name" IS NOT NULL AND length("gr_name") > (select type_digits from sys._columns where name = 'gr_name' and table_id in (select id from tables where name = 'netcdf_attrs' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."netcdf_attrs"."obj_name"' as full_col_nm, 256 as max_allowed_length, length("obj_name") as data_length, t."obj_name" as data_value FROM "sys"."netcdf_attrs" t WHERE "obj_name" IS NOT NULL AND length("obj_name") > (select type_digits from sys._columns where name = 'obj_name' and table_id in (select id from tables where name = 'netcdf_attrs' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."netcdf_dims"."name"' as full_col_nm, 64 as max_allowed_length, length("name") as data_length, t."name" as data_value FROM "sys"."netcdf_dims" t WHERE "name" IS NOT NULL AND length("name") > (select type_digits from sys._columns where name = 'name' and table_id in (select id from tables where name = 'netcdf_dims' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."netcdf_files"."location"' as full_col_nm, 256 as max_allowed_length, length("location") as data_length, t."location" as data_value FROM "sys"."netcdf_files" t WHERE "location" IS NOT NULL AND length("location") > (select type_digits from sys._columns where name = 'location' and table_id in (select id from tables where name = 'netcdf_files' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."netcdf_vars"."name"' as full_col_nm, 64 as max_allowed_length, length("name") as data_length, t."name" as data_value FROM "sys"."netcdf_vars" t WHERE "name" IS NOT NULL AND length("name") > (select type_digits from sys._columns where name = 'name' and table_id in (select id from tables where name = 'netcdf_vars' and schema_id in (select id from sys.schemas where name = 'sys')));
SELECT '"sys"."netcdf_vars"."vartype"' as full_col_nm, 64 as max_allowed_length, length("vartype") as data_length, t."vartype" as data_value FROM "sys"."netcdf_vars" t WHERE "vartype" IS NOT NULL AND length("vartype") > (select type_digits from sys._columns where name = 'vartype' and table_id in (select id from tables where name = 'netcdf_vars' and schema_id in (select id from sys.schemas where name = 'sys')));

