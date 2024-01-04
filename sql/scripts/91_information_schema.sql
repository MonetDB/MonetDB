-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

-- ISO/IEC SQL/Schemata (as defined in ISO_9075_11_Schemata_2011_E.pdf)
-- defines INFORMATION_SCHEMA schema and standardised views
--
-- NOTE 1: MonetDB does NOT support catalog qualifiers in object names, so
--         all the *CATALOG* columns in next views will always return NULL.
-- NOTE 2: Most views have been extended (after the standard columns) with
--         MonetDB specific information columns such as schema_id, table_id,
--         column_id, is_system, etc. This simplifies filtering and joins with
--         system tables/views in sys or tmp schemas when needed.

CREATE SCHEMA INFORMATION_SCHEMA;
COMMENT ON SCHEMA INFORMATION_SCHEMA IS 'ISO/IEC 9075-11 SQL/Schemata';

-- The view CHARACTER_SETS identifies the character sets available in the database.
-- MonetDB has a hardcoded character set encoding (UTF-8) for all character data.
-- It does not support multiple or alternative character sets, hence this view
-- only shows one encoding: UTF-8.
CREATE VIEW INFORMATION_SCHEMA.CHARACTER_SETS AS SELECT
  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,
  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,
  cast('UTF-8' AS varchar(16)) AS CHARACTER_SET_NAME,
  cast('ISO/IEC 10646:2021' AS varchar(20)) AS CHARACTER_REPERTOIRE,
  cast('UTF-8' AS varchar(16)) AS FORM_OF_USE,
  cast(NULL AS varchar(1)) AS DEFAULT_COLLATE_CATALOG,
  cast(NULL AS varchar(1)) AS DEFAULT_COLLATE_SCHEMA,
  cast(NULL AS varchar(1)) AS DEFAULT_COLLATE_NAME;

GRANT SELECT ON TABLE INFORMATION_SCHEMA.CHARACTER_SETS TO PUBLIC WITH GRANT OPTION;

-- The view SCHEMATA contains all schemas in the database
-- TODO: that the current user has access to (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.SCHEMATA AS SELECT
  cast(NULL AS varchar(1)) AS CATALOG_NAME,
  s."name" AS SCHEMA_NAME,
  a."name" AS SCHEMA_OWNER,
  cast(NULL AS varchar(1)) AS DEFAULT_CHARACTER_SET_CATALOG,
  cast(NULL AS varchar(1)) AS DEFAULT_CHARACTER_SET_SCHEMA,
  cast('UTF-8' AS varchar(16)) AS DEFAULT_CHARACTER_SET_NAME,
  cast(NULL AS varchar(1)) AS SQL_PATH,
  -- MonetDB column extensions
  s."id" AS schema_id,
  s."system" AS is_system,
  cm."remark" AS comments
 FROM sys."schemas" s
 INNER JOIN sys."auths" a ON s."owner" = a."id"
 LEFT OUTER JOIN sys."comments" cm ON s."id" = cm."id"
 ORDER BY s."name";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.SCHEMATA TO PUBLIC WITH GRANT OPTION;

-- The view TABLES contains all tables and views defined in the database.
-- TODO: Only those tables and views are shown that the current user has access to
-- (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.TABLES AS SELECT
  cast(NULL AS varchar(1)) AS TABLE_CATALOG,
  s."name" AS TABLE_SCHEMA,
  t."name" AS TABLE_NAME,
  tt."table_type_name" AS TABLE_TYPE,
  cast(NULL AS varchar(1)) AS SELF_REFERENCING_COLUMN_NAME,
  cast(NULL AS varchar(1)) AS REFERENCE_GENERATION,
  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_CATALOG,
  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_SCHEMA,
  cast(NULL AS varchar(1)) AS USER_DEFINED_TYPE_NAME,
  cast(sys.ifthenelse((t."type" IN (0, 3, 7, 20, 30) AND t."access" IN (0, 2)), 'YES', 'NO') AS varchar(3)) AS IS_INSERTABLE_INTO,
  cast('NO' AS varchar(3)) AS IS_TYPED,
  cast((CASE t."commit_action" WHEN 1 THEN 'DELETE' WHEN 2 THEN 'PRESERVE' WHEN 3 THEN 'DROP' ELSE NULL END) AS varchar(10)) AS COMMIT_ACTION,
  -- MonetDB column extensions
  t."schema_id" AS schema_id,
  t."id" AS table_id,
  t."type" AS table_type_id,
  st."count" AS row_count,
  t."system" AS is_system,
  sys.ifthenelse(t."type" IN (1, 11), TRUE, FALSE) AS is_view,
  t."query" AS query_def,
  cm."remark" AS comments
 FROM sys."tables" t
 INNER JOIN sys."schemas" s ON t."schema_id" = s."id"
 INNER JOIN sys."table_types" tt ON t."type" = tt."table_type_id"
 LEFT OUTER JOIN sys."comments" cm ON t."id" = cm."id"
 LEFT OUTER JOIN (SELECT DISTINCT "schema", "table", "count" FROM sys."statistics"()) st ON (s."name" = st."schema" AND t."name" = st."table")
 ORDER BY s."name", t."name";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.TABLES TO PUBLIC WITH GRANT OPTION;

-- The view VIEWS contains all views defined in the database.
-- TODO: Only those views are shown that the current user has access to
-- (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.VIEWS AS SELECT
  cast(NULL AS varchar(1)) AS TABLE_CATALOG,
  s."name" AS TABLE_SCHEMA,
  t."name" AS TABLE_NAME,
  t."query" AS VIEW_DEFINITION,
  cast('NONE' AS varchar(10)) AS CHECK_OPTION,
  cast('NO' AS varchar(3)) AS IS_UPDATABLE,
  cast('NO' AS varchar(3)) AS INSERTABLE_INTO,
  cast('NO' AS varchar(3)) AS IS_TRIGGER_UPDATABLE,
  cast('NO' AS varchar(3)) AS IS_TRIGGER_DELETABLE,
  cast('NO' AS varchar(3)) AS IS_TRIGGER_INSERTABLE_INTO,
  -- MonetDB column extensions
  t."schema_id" AS schema_id,
  t."id" AS table_id,
  cast(sys.ifthenelse(t."system", t."type" + 10 /* system view */, t."type") AS smallint) AS table_type_id,
  t."system" AS is_system,
  cm."remark" AS comments
 FROM sys."_tables" t
 INNER JOIN sys."schemas" s ON t."schema_id" = s."id"
 LEFT OUTER JOIN sys."comments" cm ON t."id" = cm."id"
 WHERE t."type" = 1
 ORDER BY s."name", t."name";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.VIEWS TO PUBLIC WITH GRANT OPTION;

-- The view COLUMNS contains information about all table columns (or view columns) in the database.
-- TODO: Only those columns are shown that the current user has access to
-- (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.COLUMNS AS SELECT
  cast(NULL AS varchar(1)) AS TABLE_CATALOG,
  s."name" AS TABLE_SCHEMA,
  t."name" AS TABLE_NAME,
  c."name" AS COLUMN_NAME,
  cast(1 + c."number" AS int) AS ORDINAL_POSITION,
  c."default" AS COLUMN_DEFAULT,
  cast(sys.ifthenelse(c."null", 'YES', 'NO') AS varchar(3)) AS IS_NULLABLE,
  cast(sys."sql_datatype"(c."type", c."type_digits", c."type_scale", true, true) AS varchar(1024)) AS DATA_TYPE,
  cast(sys.ifthenelse(c."type" IN ('varchar','clob','char','json','url','xml') AND c."type_digits" > 0, c."type_digits", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,
  cast(sys.ifthenelse(c."type" IN ('varchar','clob','char','json','url','xml') AND c."type_digits" > 0, 4 * cast(c."type_digits" as bigint), NULL) AS bigint) AS CHARACTER_OCTET_LENGTH,
  cast(sys.ifthenelse(c."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), c."type_digits", NULL) AS int) AS NUMERIC_PRECISION,
  cast(sys.ifthenelse(c."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(c."type" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,
  cast(sys.ifthenelse(c."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), c."type_scale", NULL) AS int) AS NUMERIC_SCALE,
  cast(sys.ifthenelse(c."type" IN ('date','timestamp','timestamptz','time','timetz'), sys.ifthenelse(c."type_scale" > 0, c."type_scale" -1, 0), NULL) AS int) AS DATETIME_PRECISION,
  cast(sys.ifthenelse(c."type" IN ('day_interval','month_interval','sec_interval'), sys."sql_datatype"(c."type", c."type_digits", c."type_scale", true, true), NULL) AS varchar(40)) AS INTERVAL_TYPE,
  cast(CASE c."type" WHEN 'day_interval' THEN 0 WHEN 'month_interval' THEN 0 WHEN 'sec_interval' THEN (sys.ifthenelse(c."type_digits" IN (7, 10, 12, 13), sys.ifthenelse(c."type_scale" > 0, c."type_scale", 3), 0)) ELSE NULL END AS int) AS INTERVAL_PRECISION,
  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,
  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,
  cast(sys.ifthenelse(c."type" IN ('varchar','clob','char','json','url','xml'), 'UTF-8', NULL) AS varchar(16)) AS CHARACTER_SET_NAME,
  cast(NULL AS varchar(1)) AS COLLATION_CATALOG,
  cast(NULL AS varchar(1)) AS COLLATION_SCHEMA,
  cast(NULL AS varchar(1)) AS COLLATION_NAME,
  cast(NULL AS varchar(1)) AS DOMAIN_CATALOG,
  cast(NULL AS varchar(1)) AS DOMAIN_SCHEMA,
  cast(NULL AS varchar(1)) AS DOMAIN_NAME,
  cast(NULL AS varchar(1)) AS UDT_CATALOG,
  cast(NULL AS varchar(1)) AS UDT_SCHEMA,
  cast(NULL AS varchar(1)) AS UDT_NAME,
  cast(NULL AS varchar(1)) AS SCOPE_CATALOG,
  cast(NULL AS varchar(1)) AS SCOPE_SCHEMA,
  cast(NULL AS varchar(1)) AS SCOPE_NAME,
  cast(NULL AS int) AS MAXIMUM_CARDINALITY,
  cast(NULL AS varchar(1)) AS DTD_IDENTIFIER,
  cast('NO' AS varchar(3)) AS IS_SELF_REFERENCING,
  cast(sys.ifthenelse(seq."name" IS NULL OR c."null", 'NO', 'YES') AS varchar(3)) AS IS_IDENTITY,
  seq."name" AS IDENTITY_GENERATION,
  seq."start" AS IDENTITY_START,
  seq."increment" AS IDENTITY_INCREMENT,
  seq."maxvalue" AS IDENTITY_MAXIMUM,
  seq."minvalue" AS IDENTITY_MINIMUM,
  cast(sys.ifthenelse(seq."name" IS NULL, NULL, sys.ifthenelse(seq."cycle", 'YES', 'NO')) AS varchar(3)) AS IDENTITY_CYCLE,
  cast(sys.ifthenelse(seq."name" IS NULL, 'NO', 'YES') AS varchar(3)) AS IS_GENERATED,
  cast(sys.ifthenelse(seq."name" IS NULL, NULL, c."default") AS varchar(1024)) AS GENERATION_EXPRESSION,
  cast('NO' AS varchar(3)) AS IS_SYSTEM_TIME_PERIOD_START,
  cast('NO' AS varchar(3)) AS IS_SYSTEM_TIME_PERIOD_END,
  cast('NO' AS varchar(3)) AS SYSTEM_TIME_PERIOD_TIMESTAMP_GENERATION,
  cast(sys.ifthenelse(t."type" IN (0,3,7,20,30), 'YES', 'NO') AS varchar(3)) AS IS_UPDATABLE,
  cast(NULL AS varchar(1)) AS DECLARED_DATA_TYPE,
  cast(NULL AS int) AS DECLARED_NUMERIC_PRECISION,
  cast(NULL AS int) AS DECLARED_NUMERIC_SCALE,
  -- MonetDB column extensions
  t."schema_id" AS schema_id,
  c."table_id" AS table_id,
  c."id" AS column_id,
  seq."id" AS sequence_id,
  t."system" AS is_system,
  cm."remark" AS comments
 FROM sys."columns" c
 INNER JOIN sys."tables" t ON c."table_id" = t."id"
 INNER JOIN sys."schemas" s ON t."schema_id" = s."id"
 LEFT OUTER JOIN sys."comments" cm ON c."id" = cm."id"
 LEFT OUTER JOIN sys."sequences" seq ON ((seq."name"||'"') = substring(c."default", 3 + sys."locate"('"."seq_',c."default",14)))	-- match only sequences of generated identity columns
 ORDER BY s."name", t."name", c."number";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.COLUMNS TO PUBLIC WITH GRANT OPTION;

-- The view CHECK_CONSTRAINTS contains all check constraints defined on a table,
-- that are owned by a currently enabled role.
-- This view is currently empty as MonetDB does not support CHECK constraints yet.
CREATE VIEW INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS SELECT
  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,
  cast(NULL AS varchar(1024)) AS CONSTRAINT_SCHEMA,
  cast(NULL AS varchar(1024)) AS CONSTRAINT_NAME,
  cast(NULL AS varchar(1024)) AS CHECK_CLAUSE
 WHERE 1=0;

GRANT SELECT ON TABLE INFORMATION_SCHEMA.CHECK_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;

-- The view TABLE_CONSTRAINTS contains all constraints belonging to tables
-- TODO: that the current user owns or has some privilege other than SELECT on.
CREATE VIEW INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS SELECT
  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,
  s."name" AS CONSTRAINT_SCHEMA,
  k."name" AS CONSTRAINT_NAME,
  cast(NULL AS varchar(1)) AS TABLE_CATALOG,
  s."name" AS TABLE_SCHEMA,
  t."name" AS TABLE_NAME,
  cast(CASE k."type" WHEN 0 THEN 'PRIMARY KEY' WHEN 1 THEN 'UNIQUE' WHEN 2 THEN 'FOREIGN KEY' ELSE NULL END AS varchar(16)) AS CONSTRAINT_TYPE,
  cast('NO' AS varchar(3)) AS IS_DEFERRABLE,
  cast('NO' AS varchar(3)) AS INITIALLY_DEFERRED,
  cast('YES' AS varchar(3)) AS ENFORCED,
  -- MonetDB column extensions
  t."schema_id" AS schema_id,
  t."id" AS table_id,
  k."id" AS key_id,
  k."type" AS key_type,
  t."system" AS is_system
 FROM (SELECT sk."id", sk."table_id", sk."name", sk."type" FROM sys."keys" sk UNION ALL SELECT tk."id", tk."table_id", tk."name", tk."type" FROM tmp."keys" tk) k
 INNER JOIN (SELECT st."id", st."schema_id", st."name", st."system" FROM sys."_tables" st UNION ALL SELECT tt."id", tt."schema_id", tt."name", tt."system" FROM tmp."_tables" tt) t ON k."table_id" = t."id"
 INNER JOIN sys."schemas" s ON t."schema_id" = s."id"
 ORDER BY s."name", t."name", k."name";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.TABLE_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;

-- The view REFERENTIAL_CONSTRAINTS contains all referential (foreign key) constraints in the current database.
-- TODO: Only those constraints are shown for which the current user has write access to the referencing table
-- (by way of being the owner or having some privilege other than SELECT).
CREATE VIEW INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS SELECT
  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,
  s."name" AS CONSTRAINT_SCHEMA,
  fk."name" AS CONSTRAINT_NAME,
  cast(NULL AS varchar(1)) AS UNIQUE_CONSTRAINT_CATALOG,
  uks."name" AS UNIQUE_CONSTRAINT_SCHEMA,
  uk."name" AS UNIQUE_CONSTRAINT_NAME,
  cast('FULL' AS varchar(7)) AS MATCH_OPTION,
  fk."update_action" AS UPDATE_RULE,
  fk."delete_action" AS DELETE_RULE,
  -- MonetDB column extensions
  t."schema_id" AS fk_schema_id,
  t."id" AS fk_table_id,
  t."name" AS fk_table_name,
  fk."id" AS fk_key_id,
  ukt."schema_id" AS uc_schema_id,
  uk."table_id" AS uc_table_id,
  ukt."name" AS uc_table_name,
  uk."id" AS uc_key_id
 FROM sys."fkeys" fk
 INNER JOIN sys."tables" t ON t."id" = fk."table_id"
 INNER JOIN sys."schemas" s ON s."id" = t."schema_id"
 LEFT OUTER JOIN sys."keys" uk ON uk."id" = fk."rkey"
 LEFT OUTER JOIN sys."tables" ukt ON ukt."id" = uk."table_id"
 LEFT OUTER JOIN sys."schemas" uks ON uks."id" = ukt."schema_id"
 ORDER BY s."name", t."name", fk."name";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;

-- The view ROUTINES contains all functions and procedures in the current database.
-- TODO: Only those functions and procedures are shown that the current user has access to
-- (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.ROUTINES AS SELECT
  cast(NULL AS varchar(1)) AS SPECIFIC_CATALOG,
  s."name" AS SPECIFIC_SCHEMA,
  cast(f."name"||'('||f."id"||')' AS varchar(270)) AS SPECIFIC_NAME, -- TODO: replace with full routine signature string. Note sys.fully_qualified_functions.nme does not produce the correct signature.
  cast(NULL AS varchar(1)) AS ROUTINE_CATALOG,
  s."name" AS ROUTINE_SCHEMA,
  f."name" AS ROUTINE_NAME,
  ft."function_type_keyword" AS ROUTINE_TYPE,
  cast(NULL AS varchar(1)) AS MODULE_CATALOG,
  cast(NULL AS varchar(1)) AS MODULE_SCHEMA,
  cast(f."mod" AS varchar(128)) AS MODULE_NAME,
  cast(NULL AS varchar(1)) AS UDT_CATALOG,
  cast(NULL AS varchar(1)) AS UDT_SCHEMA,
  cast(NULL AS varchar(1)) AS UDT_NAME,
  cast(CASE f."type" WHEN 1 THEN sys."sql_datatype"(a."type", a."type_digits", a."type_scale", true, true) WHEN 2 THEN NULL WHEN 5 THEN 'TABLE' WHEN 7 THEN 'TABLE' ELSE NULL END AS varchar(1024)) AS DATA_TYPE,
  cast(sys.ifthenelse(a."type" IN ('varchar','clob','char','json','url','xml') AND a."type_digits" > 0, a."type_digits", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,
  cast(sys.ifthenelse(a."type" IN ('varchar','clob','char','json','url','xml') AND a."type_digits" > 0, 4 * cast(a."type_digits" as bigint), NULL) AS bigint) AS CHARACTER_OCTET_LENGTH,
  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,
  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,
  'UTF-8' AS CHARACTER_SET_NAME,
  cast(NULL AS varchar(1)) AS COLLATION_CATALOG,
  cast(NULL AS varchar(1)) AS COLLATION_SCHEMA,
  cast(NULL AS varchar(1)) AS COLLATION_NAME,
  cast(sys.ifthenelse(a."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a."type_digits", NULL) AS int) AS NUMERIC_PRECISION,
  cast(sys.ifthenelse(a."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(a."type" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,
  cast(sys.ifthenelse(a."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a."type_scale", NULL) AS int) AS NUMERIC_SCALE,
  cast(sys.ifthenelse(a."type" IN ('date','timestamp','timestamptz','time','timetz'), a."type_scale" -1, NULL) AS int) AS DATETIME_PRECISION,
  cast(sys.ifthenelse(a."type" IN ('day_interval','month_interval','sec_interval'), sys."sql_datatype"(a."type", a."type_digits", a."type_scale", true, true), NULL) AS varchar(40)) AS INTERVAL_TYPE,
  cast(CASE a."type" WHEN 'day_interval' THEN 0 WHEN 'month_interval' THEN 0 WHEN 'sec_interval' THEN (sys.ifthenelse(a."type_digits" IN (7, 10, 12, 13), sys.ifthenelse(a."type_scale" > 0, a."type_scale", 3), 0)) ELSE NULL END AS int) AS INTERVAL_PRECISION,
  cast(NULL AS varchar(1)) AS TYPE_UDT_CATALOG,
  cast(NULL AS varchar(1)) AS TYPE_UDT_SCHEMA,
  cast(NULL AS varchar(1)) AS TYPE_UDT_NAME,
  cast(NULL AS varchar(1)) AS SCOPE_CATALOG,
  cast(NULL AS varchar(1)) AS SCOPE_SCHEMA,
  cast(NULL AS varchar(1)) AS SCOPE_NAME,
  cast(NULL AS int) AS MAXIMUM_CARDINALITY,
  cast(NULL AS int) AS DTD_IDENTIFIER,
  cast(sys."ifthenelse"(sys."locate"('begin',f."func") > 0, sys."ifthenelse"(sys."endswith"(f."func",';'), sys."substring"(f."func", sys."locate"('begin',f."func"), sys."length"(sys."substring"(f."func", sys."locate"('begin',f."func")))-1), sys."substring"(f."func", sys."locate"('begin',f."func"))), NULL) AS varchar(8196)) AS ROUTINE_BODY,
  f."func" AS ROUTINE_DEFINITION,
  cast(sys."ifthenelse"(sys."locate"('external name',f."func") > 0, sys."ifthenelse"(sys."endswith"(f."func",';'), sys."substring"(f."func", 14 + sys."locate"('external name',f."func"), sys."length"(sys."substring"(f."func", 14 + sys."locate"('external name',f."func")))-1), sys."substring"(f."func", 14 + sys."locate"('external name',f."func"))), NULL) AS varchar(1024)) AS EXTERNAL_NAME,
  fl."language_keyword" AS EXTERNAL_LANGUAGE,
  'GENERAL' AS PARAMETER_STYLE,
  'YES' AS IS_DETERMINISTIC,
  cast(sys.ifthenelse(f."side_effect", 'MODIFIES', 'READ') AS varchar(10)) AS SQL_DATA_ACCESS,
  cast(CASE f."type" WHEN 2 THEN NULL ELSE 'NO' END AS varchar(3)) AS IS_NULL_CALL,
  cast(NULL AS varchar(1)) AS SQL_PATH,
  cast(NULL AS varchar(1)) AS SCHEMA_LEVEL_ROUTINE,
  cast(NULL AS int) AS MAX_DYNAMIC_RESULT_SETS,
  cast(NULL AS varchar(1)) AS IS_USER_DEFINED_CAST,
  cast(NULL AS varchar(1)) AS IS_IMPLICITLY_INVOCABLE,
  cast(NULL AS varchar(1)) AS SECURITY_TYPE,
  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_CATALOG,
  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_SCHEMA,
  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_NAME,
  cast(NULL AS varchar(1)) AS AS_LOCATOR,
  cast(NULL AS timestamp) AS CREATED,
  cast(NULL AS timestamp) AS LAST_ALTERED,
  cast(NULL AS varchar(1)) AS NEW_SAVEPOINT_LEVEL,
  cast(NULL AS varchar(1)) AS IS_UDT_DEPENDENT,
  cast(NULL AS varchar(1)) AS RESULT_CAST_FROM_DATA_TYPE,
  cast(NULL AS varchar(1)) AS RESULT_CAST_AS_LOCATOR,
  cast(NULL AS int) AS RESULT_CAST_CHAR_MAX_LENGTH,
  cast(NULL AS int) AS RESULT_CAST_CHAR_OCTET_LENGTH,
  cast(NULL AS varchar(1)) AS RESULT_CAST_CHAR_SET_CATALOG,
  cast(NULL AS varchar(1)) AS RESULT_CAST_CHAR_SET_SCHEMA,
  cast(NULL AS varchar(1)) AS RESULT_CAST_CHARACTER_SET_NAME,
  cast(NULL AS varchar(1)) AS RESULT_CAST_COLLATION_CATALOG,
  cast(NULL AS varchar(1)) AS RESULT_CAST_COLLATION_SCHEMA,
  cast(NULL AS varchar(1)) AS RESULT_CAST_COLLATION_NAME,
  cast(NULL AS int) AS RESULT_CAST_NUMERIC_PRECISION,
  cast(NULL AS int) AS RESULT_CAST_NUMERIC_RADIX,
  cast(NULL AS int) AS RESULT_CAST_NUMERIC_SCALE,
  cast(NULL AS int) AS RESULT_CAST_DATETIME_PRECISION,
  cast(NULL AS varchar(1)) AS RESULT_CAST_INTERVAL_TYPE,
  cast(NULL AS int) AS RESULT_CAST_INTERVAL_PRECISION,
  cast(NULL AS varchar(1)) AS RESULT_CAST_TYPE_UDT_CATALOG,
  cast(NULL AS varchar(1)) AS RESULT_CAST_TYPE_UDT_SCHEMA,
  cast(NULL AS varchar(1)) AS RESULT_CAST_TYPE_UDT_NAME,
  cast(NULL AS varchar(1)) AS RESULT_CAST_SCOPE_CATALOG,
  cast(NULL AS varchar(1)) AS RESULT_CAST_SCOPE_SCHEMA,
  cast(NULL AS varchar(1)) AS RESULT_CAST_SCOPE_NAME,
  cast(NULL AS int) AS RESULT_CAST_MAX_CARDINALITY,
  cast(NULL AS varchar(1)) AS RESULT_CAST_DTD_IDENTIFIER,
  cast(NULL AS varchar(1)) AS DECLARED_DATA_TYPE,
  cast(NULL AS int) AS DECLARED_NUMERIC_PRECISION,
  cast(NULL AS int) AS DECLARED_NUMERIC_SCALE,
  cast(NULL AS varchar(1)) AS RESULT_CAST_FROM_DECLARED_DATA_TYPE,
  cast(NULL AS int) AS RESULT_CAST_DECLARED_NUMERIC_PRECISION,
  cast(NULL AS int) AS RESULT_CAST_DECLARED_NUMERIC_SCALE,
  -- MonetDB column extensions
  f."schema_id" AS schema_id,
  f."id" AS function_id,
  f."type" AS function_type,
  f."language" AS function_language,
  f."system" AS is_system,
  cm."remark" AS comments
 FROM sys."functions" f
 INNER JOIN sys."schemas" s ON s."id" = f."schema_id"
 INNER JOIN sys."function_types" ft ON ft."function_type_id" = f."type"
 INNER JOIN sys."function_languages" fl ON fl."language_id" = f."language"
 LEFT OUTER JOIN sys."args" a ON a."func_id" = f."id" and a."inout" = 0 and a."number" = 0
 LEFT OUTER JOIN sys."comments" cm ON cm."id" = f."id"
 WHERE f."type" in (1, 2, 5, 7) -- 1=Scalar function, 2=Procedure, 5=Function returning a table, 7=Loader function
 ORDER BY s."name", f."name";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.ROUTINES TO PUBLIC WITH GRANT OPTION;

-- The view PARAMETERS contains information about the parameters (arguments) of
-- all ROUTINES (functions and procedures) in the current database.
-- TODO: Only those routine parameters are shown that the current user has
-- access to (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.PARAMETERS AS SELECT
  cast(NULL AS varchar(1)) AS SPECIFIC_CATALOG,
  s."name" AS SPECIFIC_SCHEMA,
  cast(f."name"||'('||f."id"||')' AS varchar(270)) AS SPECIFIC_NAME, -- TODO: replace with full routine signature string. Note sys.fully_qualified_functions.nme does not produce the correct signature.
  cast(sys.ifthenelse((a."inout" = 0 OR f."type" = 2), 1 + a."number", sys.ifthenelse(f."type" = 1, a."number", (1 + a."number" - f.count_out_cols))) AS int) AS ORDINAL_POSITION,
  cast(sys.ifthenelse(a."inout" = 0, 'OUT', sys.ifthenelse(a."inout" = 1, 'IN', 'INOUT')) as varchar(5)) AS PARAMETER_MODE,  -- we do not yet support INOUT
  cast(sys.ifthenelse(a."inout" = 0, 'YES', 'NO') as varchar(3)) AS IS_RESULT,
  cast(NULL AS varchar(1)) AS AS_LOCATOR,
  a."name" AS PARAMETER_NAME,
  cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_CATALOG,
  cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_SCHEMA,
  cast(NULL AS varchar(1)) AS FROM_SQL_SPECIFIC_NAME,
  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_CATALOG,
  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_SCHEMA,
  cast(NULL AS varchar(1)) AS TO_SQL_SPECIFIC_NAME,
  cast(sys."sql_datatype"(a."type", a."type_digits", a."type_scale", true, true) AS varchar(1024)) AS DATA_TYPE,
  cast(sys.ifthenelse(a."type" IN ('varchar','clob','char','json','url','xml') AND a."type_digits" > 0, a."type_digits", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,
  cast(sys.ifthenelse(a."type" IN ('varchar','clob','char','json','url','xml') AND a."type_digits" > 0, 4 * cast(a."type_digits" as bigint), NULL) AS bigint) AS CHARACTER_OCTET_LENGTH,
  cast(NULL AS varchar(1)) AS CHARACTER_SET_CATALOG,
  cast(NULL AS varchar(1)) AS CHARACTER_SET_SCHEMA,
  cast(sys.ifthenelse(a."type" IN ('varchar','clob','char','json','url','xml'), 'UTF-8', NULL) AS varchar(16)) AS CHARACTER_SET_NAME,
  cast(NULL AS varchar(1)) AS COLLATION_CATALOG,
  cast(NULL AS varchar(1)) AS COLLATION_SCHEMA,
  cast(NULL AS varchar(1)) AS COLLATION_NAME,
  cast(sys.ifthenelse(a."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a."type_digits", NULL) AS int) AS NUMERIC_PRECISION,
  cast(sys.ifthenelse(a."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(a."type" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,
  cast(sys.ifthenelse(a."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), a."type_scale", NULL) AS int) AS NUMERIC_SCALE,
  cast(sys.ifthenelse(a."type" IN ('date','timestamp','timestamptz','time','timetz'), sys.ifthenelse(a."type_scale" > 0, a."type_scale" -1, 0), NULL) AS int) AS DATETIME_PRECISION,
  cast(sys.ifthenelse(a."type" IN ('day_interval','month_interval','sec_interval'), sys."sql_datatype"(a."type", a."type_digits", a."type_scale", true, true), NULL) AS varchar(40)) AS INTERVAL_TYPE,
  cast(CASE a."type" WHEN 'day_interval' THEN 0 WHEN 'month_interval' THEN 0 WHEN 'sec_interval' THEN (sys.ifthenelse(a."type_digits" IN (7, 10, 12, 13), sys.ifthenelse(a."type_scale" > 0, a."type_scale", 3), 0)) ELSE NULL END AS int) AS INTERVAL_PRECISION,
  cast(NULL AS varchar(1)) AS UDT_CATALOG,
  cast(NULL AS varchar(1)) AS UDT_SCHEMA,
  cast(NULL AS varchar(1)) AS UDT_NAME,
  cast(NULL AS varchar(1)) AS SCOPE_CATALOG,
  cast(NULL AS varchar(1)) AS SCOPE_SCHEMA,
  cast(NULL AS varchar(1)) AS SCOPE_NAME,
  cast(NULL AS int) AS MAXIMUM_CARDINALITY,
  cast(NULL AS varchar(1)) AS DTD_IDENTIFIER,
  cast(NULL AS varchar(1)) AS DECLARED_DATA_TYPE,
  cast(NULL AS int) AS DECLARED_NUMERIC_PRECISION,
  cast(NULL AS int) AS DECLARED_NUMERIC_SCALE,
  cast(NULL AS varchar(1)) AS PARAMETER_DEFAULT,
  -- MonetDB column extensions
  f."schema_id" AS schema_id,
  f."id" AS function_id,
  a."id" AS arg_id,
  f."name" AS function_name,
  f."type" AS function_type,
  f."system" AS is_system
 FROM sys."args" a
 INNER JOIN (SELECT fun.*, (select count(*) from sys.args a0 where a0.inout = 0 and a0.func_id = fun.id) as count_out_cols FROM sys."functions" fun WHERE fun."type" in (1, 2, 5, 7)) f ON f."id" = a."func_id"
 INNER JOIN sys."schemas" s ON s."id" = f."schema_id"
 ORDER BY s."name", f."name", f."id", a."inout" DESC, a."number";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.PARAMETERS TO PUBLIC WITH GRANT OPTION;

-- The view SEQUENCES contains all sequences defined in the current database.
-- TODO: Only those sequences are shown that the current user has access to
-- (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.SEQUENCES AS SELECT
  cast(NULL AS varchar(1)) AS SEQUENCE_CATALOG,
  s."name" AS SEQUENCE_SCHEMA,
  sq."name" AS SEQUENCE_NAME,
  cast('BIGINT' AS varchar(16)) AS DATA_TYPE,
  cast(64 AS SMALLINT) AS NUMERIC_PRECISION,
  cast(2 AS SMALLINT) AS NUMERIC_PRECISION_RADIX,
  cast(0 AS SMALLINT) AS NUMERIC_SCALE,
  sq."start" AS START_VALUE,
  sq."minvalue" AS MINIMUM_VALUE,
  sq."maxvalue" AS MAXIMUM_VALUE,
  sq."increment" AS INCREMENT,
  cast(sys.ifthenelse(sq."cycle", 'YES', 'NO') AS varchar(3)) AS CYCLE_OPTION,
  cast(NULL AS varchar(16)) AS DECLARED_DATA_TYPE,
  cast(NULL AS SMALLINT) AS DECLARED_NUMERIC_PRECISION,
  cast(NULL AS SMALLINT) AS DECLARED_NUMERIC_SCALE,
  -- MonetDB column extensions
  sq."schema_id" AS schema_id,
  sq."id" AS sequence_id,
  get_value_for(s."name", sq."name") AS current_value,
  sq."cacheinc" AS cacheinc,
  cm."remark" AS comments
 FROM sys."sequences" sq
 INNER JOIN sys."schemas" s ON sq."schema_id" = s."id"
 LEFT OUTER JOIN sys."comments" cm ON sq."id" = cm."id"
 ORDER BY s."name", sq."name";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.SEQUENCES TO PUBLIC WITH GRANT OPTION;

