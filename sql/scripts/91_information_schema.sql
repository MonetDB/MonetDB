-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.

-- ISO/IEC SQL/Schemata (as defined in ISO_9075_11_Schemata_2011_E.pdf)
-- defines INFORMATION_SCHEMA schema and standardised views
--
-- NOTE 1: The views have been extended with MonetDB specific information
--         columns such as schema_id, table_id, column_id, is_system, etc.
--         This eases joins with any sys.* tables/views.
-- NOTE 2: MonetDB does NOT support catalog qualifiers in object names, so
--         all the *CATALOG* columns in next views will allways contain NULL.

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

-- The view SCHEMATA contains all schemas in the database that the
-- current user has access to (by way of being the owner or having some privilege).
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
-- Only those tables and views are shown that the current user has access to
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
-- Only those views are shown that the current user has access to
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
-- Only those columns are shown that the current user has access to
-- (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.COLUMNS AS SELECT
  cast(NULL AS varchar(1)) AS TABLE_CATALOG,
  s."name" AS TABLE_SCHEMA,
  t."name" AS TABLE_NAME,
  c."name" AS COLUMN_NAME,
  cast(c."number" +1 AS int) AS ORDINAL_POSITION,
  c."default" AS COLUMN_DEFAULT,
  cast(sys.ifthenelse(c."null", 'YES', 'NO') AS varchar(3)) AS IS_NULLABLE,
  CASE c."type" WHEN 'day_interval' THEN 'interval day' WHEN 'month_interval' THEN 'interval month' WHEN 'sec_interval' THEN 'interval second' ELSE c."type" END AS DATA_TYPE,
  cast(sys.ifthenelse(c."type" IN ('varchar','clob','char','json','url','xml'), c."type_digits", NULL) AS int) AS CHARACTER_MAXIMUM_LENGTH,
  cast(sys.ifthenelse(c."type" IN ('varchar','clob','char','json','url','xml'), c."type_digits" * 3, NULL) AS int) AS CHARACTER_OCTET_LENGTH,
  cast(sys.ifthenelse(c."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), c."type_digits", NULL) AS int) AS NUMERIC_PRECISION,
  cast(sys.ifthenelse(c."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','oid'), 2, sys.ifthenelse(c."type" IN ('decimal','numeric'), 10, NULL)) AS int) AS NUMERIC_PRECISION_RADIX,
  cast(sys.ifthenelse(c."type" IN ('int','smallint','tinyint','bigint','hugeint','float','real','double','decimal','numeric','oid'), c."type_scale", NULL) AS int) AS NUMERIC_SCALE,
  cast(sys.ifthenelse(c."type" IN ('date','timestamp','timestamptz','time','timetz'), c."type_scale" -1, NULL) AS int) AS DATETIME_PRECISION,
  cast(CASE c."type" WHEN 'day_interval' THEN 'interval day' WHEN 'month_interval' THEN (CASE c."type_digits" WHEN 1 THEN 'interval year' WHEN 2 THEN 'interval year to month' WHEN 3 THEN 'interval month' ELSE NULL END) WHEN 'sec_interval' THEN (CASE c."type_digits" WHEN 5 THEN 'interval day to hour' WHEN 6 THEN 'interval day to minute' WHEN 7 THEN 'interval day to second' WHEN 8 THEN 'interval hour' WHEN 9 THEN 'interval hour to minute' WHEN 10 THEN 'interval hour to second' WHEN 11 THEN 'interval minute' WHEN 12 THEN 'interval minute to second' WHEN 13 THEN 'interval second' ELSE NULL END) ELSE NULL END AS varchar(40)) AS INTERVAL_TYPE,
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
  cast(CASE WHEN c."default" LIKE 'next value for %' THEN 'YES' ELSE 'NO' END AS varchar(3)) AS IS_IDENTITY,
  cast(NULL AS varchar(10)) AS IDENTITY_GENERATION,
  cast(NULL AS int) AS IDENTITY_START,
  cast(NULL AS int) AS IDENTITY_INCREMENT,
  cast(NULL AS int) AS IDENTITY_MAXIMUM,
  cast(NULL AS int) AS IDENTITY_MINIMUM,
  cast(NULL AS varchar(3)) AS IDENTITY_CYCLE,
  cast('NO' AS varchar(3)) AS IS_GENERATED,
  cast(NULL AS varchar(1)) AS GENERATION_EXPRESSION,
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
  t."system" AS is_system,
  cm."remark" AS comments
 FROM sys."columns" c
 INNER JOIN sys."tables" t ON c."table_id" = t."id"
 INNER JOIN sys."schemas" s ON t."schema_id" = s."id"
 LEFT OUTER JOIN sys."comments" cm ON c."id" = cm."id"
 ORDER BY s."name", t."name", c."number";

GRANT SELECT ON TABLE INFORMATION_SCHEMA.COLUMNS TO PUBLIC WITH GRANT OPTION;

-- The view CHECK_CONSTRAINTS contains all check constraints defined on a table,
-- that are owned by a currently enabled role.
-- This view is currently empty as MonetDB doesn't support CHECK constraints yet.
CREATE VIEW INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS SELECT
  cast(NULL AS varchar(1)) AS CONSTRAINT_CATALOG,
  cast(NULL AS varchar(1024)) AS CONSTRAINT_SCHEMA,
  cast(NULL AS varchar(1024)) AS CONSTRAINT_NAME,
  cast(NULL AS varchar(1024)) AS CHECK_CLAUSE
 WHERE 1=0;

GRANT SELECT ON TABLE INFORMATION_SCHEMA.CHECK_CONSTRAINTS TO PUBLIC WITH GRANT OPTION;

-- The view TABLE_CONSTRAINTS contains all constraints belonging to tables
-- that the current user owns or has some privilege other than SELECT on.
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

-- The view SEQUENCES contains all sequences defined in the current database.
-- Only those sequences are shown that the current user has access to
-- (by way of being the owner or having some privilege).
CREATE VIEW INFORMATION_SCHEMA.SEQUENCES AS SELECT
  cast(NULL AS varchar(1)) AS SEQUENCE_CATALOG,
  s."name" AS SEQUENCE_SCHEMA,
  sq."name" AS SEQUENCE_NAME,
  cast('bigint' AS varchar(16)) AS DATA_TYPE,
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

