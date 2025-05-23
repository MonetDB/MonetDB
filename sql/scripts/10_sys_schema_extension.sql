-- SPDX-License-Identifier: MPL-2.0
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 2024, 2025 MonetDB Foundation;
-- Copyright August 2008 - 2023 MonetDB B.V.;
-- Copyright 1997 - July 2008 CWI.

-- create additional tables in "sys" schema and fill them with static content

CREATE TABLE sys.keywords (
    keyword VARCHAR(40) NOT NULL PRIMARY KEY);

INSERT INTO sys.keywords (keyword) VALUES
  ('ADD'),
  ('ADMIN'),
  ('AFTER'),
  ('AGGREGATE'),
  ('ALL'),
  ('ALTER'),
  ('ALWAYS'),
  ('ANALYZE'),
  ('AND'),
  ('ANY'),
  ('ASC'),
  ('ASYMMETRIC'),
  ('AT'),
  ('ATOMIC'),
  ('AUTHORIZATION'),
  ('AUTO_INCREMENT'),
  ('BEFORE'),
  ('BEGIN'),
  ('BEST'),
  ('BETWEEN'),
  ('BIG'),
  ('BIGINT'),
  ('BIGSERIAL'),
  ('BINARY'),
  ('BLOB'),
  ('BY'),
  ('CACHE'),
  ('CALL'),
  ('CASCADE'),
  ('CASE'),
  ('CAST'),
  ('CENTURY'),
  ('CHAIN'),
  ('CHAR'),
  ('CHARACTER'),
  ('CHECK'),
  ('CLIENT'),
  ('CLOB'),
  ('COALESCE'),
  ('COLUMN'),
  ('COMMENT'),
  ('COMMIT'),
  ('COMMITTED'),
  ('CONSTRAINT'),
  ('CONTINUE'),
  ('CONVERT'),
  ('COPY'),
  ('CORRESPONDING'),
  ('CREATE'),
  ('CROSS'),
  ('CUBE'),
  ('CURRENT'),
  ('CURRENT_DATE'),
  ('CURRENT_ROLE'),
  ('CURRENT_SCHEMA'),
  ('CURRENT_TIME'),
  ('CURRENT_TIMESTAMP'),
  ('CURRENT_TIMEZONE'),
  ('CURRENT_USER'),
  ('CYCLE'),
  ('DATA'),
  ('DATE'),
  ('DAY'),
  ('DEALLOCATE'),
  ('DEBUG'),
  ('DEC'),
  ('DECADE'),
  ('DECIMAL'),
  ('DECLARE'),
  ('DEFAULT'),
  ('DELETE'),
  ('DELIMITERS'),
  ('DESC'),
  ('DIAGNOSTICS'),
  ('DISTINCT'),
  ('DO'),
  ('DOUBLE'),
  ('DOW'),
  ('DOY'),
  ('DROP'),
  ('EACH'),
  ('EFFORT'),
  ('ELSE'),
  ('ELSEIF'),
  ('ENCRYPTED'),
  ('END'),
  ('ENDIAN'),
  ('EPOCH'),
  ('ESCAPE'),
  ('EVERY'),
  ('EXCEPT'),
  ('EXCLUDE'),
  ('EXEC'),
  ('EXECUTE'),
  ('EXISTS'),
  ('EXPLAIN'),
  ('EXTERNAL'),
  ('EXTRACT'),
  ('FALSE'),
  ('FIRST'),
  ('FLOAT'),
  ('FOLLOWING'),
  ('FOR'),
  ('FOREIGN'),
  ('FROM'),
  ('FULL'),
  ('FUNCTION'),
  ('FWF'),
  ('GENERATED'),
  ('GLOBAL'),
  ('GRANT'),
  ('GROUP'),
  ('GROUPING'),
  ('GROUPS'),
  ('HAVING'),
  ('HOUR'),
  ('HUGEINT'),
  ('IDENTITY'),
  ('IF'),
  ('ILIKE'),
  ('IMPRINTS'),
  ('IN'),
  ('INCREMENT'),
  ('INDEX'),
  ('INNER'),
  ('INSERT'),
  ('INT'),
  ('INTEGER'),
  ('INTERSECT'),
  ('INTERVAL'),
  ('INTO'),
  ('IS'),
  ('ISOLATION'),
  ('JOIN'),
  ('KEY'),
  ('LANGUAGE'),
  ('LARGE'),
  ('LAST'),
  ('LATERAL'),
  ('LEFT'),
  ('LEVEL'),
  ('LIKE'),
  ('LIMIT'),
  ('LITTLE'),
  ('LOADER'),
  ('LOCAL'),
  ('LOCALTIME'),
  ('LOCALTIMESTAMP'),
  ('MATCH'),
  ('MATCHED'),
  ('MAXVALUE'),
  ('MEDIUMINT'),
  ('MERGE'),
  ('MINUTE'),
  ('MINVALUE'),
  ('MONTH'),
  ('NAME'),
  ('NATIVE'),
  ('NATURAL'),
  ('NEW'),
  ('NEXT'),
  ('NO'),
  ('NOT'),
  ('NOW'),
  ('NULL'),
  ('NULLIF'),
  ('NULLS'),
  ('NUMERIC'),
  ('OBJECT'),
  ('OF'),
  ('OFFSET'),
  ('OLD'),
  ('ON'),
  ('ONLY'),
  ('OPTION'),
  ('OPTIONS'),
  ('OR'),
  ('ORDER'),
  ('ORDERED'),
  ('OTHERS'),
  ('OUTER'),
  ('OVER'),
  ('PARTIAL'),
  ('PARTITION'),
  ('PASSWORD'),
  ('PATH'),
  ('PLAN'),
  ('POSITION'),
  ('PRECEDING'),
  ('PRECISION'),
  ('PREP'),
  ('PREPARE'),
  ('PRESERVE'),
  ('PRIMARY'),
  ('PRIVILEGES'),
  ('PROCEDURE'),
  ('PUBLIC'),
  ('QUARTER'),
  ('RANGE'),
  ('READ'),
  ('REAL'),
  ('RECORDS'),
  ('REFERENCES'),
  ('REFERENCING'),
  ('RELEASE'),
  ('REMOTE'),
  ('RENAME'),
  ('REPEATABLE'),
  ('REPLACE'),
  ('REPLICA'),
  ('RESTART'),
  ('RESTRICT'),
  ('RETURN'),
  ('RETURNS'),
  ('REVOKE'),
  ('RIGHT'),
  ('ROLE'),
  ('ROLLBACK'),
  ('ROLLUP'),
  ('ROW'),
  ('ROWS'),
  ('SAMPLE'),
  ('SAVEPOINT'),
  ('SCHEMA'),
  ('SECOND'),
  ('SEED'),
  ('SELECT'),
  ('SEQUENCE'),
  ('SERIAL'),
  ('SERIALIZABLE'),
  ('SERVER'),
  ('SESSION'),
  ('SESSION_USER'),
  ('SET'),
  ('SETS'),
  ('SIMPLE'),
  ('SIZE'),
  ('SMALLINT'),
  ('SOME'),
  ('SPLIT_PART'),
  ('START'),
  ('STATEMENT'),
  ('STDIN'),
  ('STDOUT'),
  ('STORAGE'),
  ('STRING'),
  ('SUBSTRING'),
  ('SYMMETRIC'),
  ('TABLE'),
  ('TEMP'),
  ('TEMPORARY'),
  ('TEXT'),
  ('THEN'),
  ('TIES'),
  ('TIME'),
  ('TIMESTAMP'),
  ('TINYINT'),
  ('TO'),
  ('TRACE'),
  ('TRANSACTION'),
  ('TRIGGER'),
  ('TRUE'),
  ('TRUNCATE'),
  ('TYPE'),
  ('UNBOUNDED'),
  ('UNCOMMITTED'),
  ('UNENCRYPTED'),
  ('UNION'),
  ('UNIQUE'),
  ('UPDATE'),
  ('USER'),
  ('USING'),
  ('VALUE'),
  ('VALUES'),
  ('VARCHAR'),
  ('VARYING'),
  ('VIEW'),
  ('WEEK'),
  ('WHEN'),
  ('WHERE'),
  ('WHILE'),
  ('WINDOW'),
  ('WITH'),
  ('WORK'),
  ('WRITE'),
  ('XMLAGG'),
  ('XMLATTRIBUTES'),
  ('XMLCOMMENT'),
  ('XMLCONCAT'),
  ('XMLDOCUMENT'),
  ('XMLELEMENT'),
  ('XMLFOREST'),
  ('XMLNAMESPACES'),
  ('XMLPARSE'),
  ('XMLPI'),
  ('XMLQUERY'),
  ('XMLSCHEMA'),
  ('XMLTEXT'),
  ('XMLVALIDATE'),
  ('YEAR'),
  ('ZONE');

ALTER TABLE sys.keywords SET READ ONLY;
GRANT SELECT ON sys.keywords TO PUBLIC;


CREATE TABLE sys.table_types (
    table_type_id   SMALLINT NOT NULL PRIMARY KEY,
    table_type_name VARCHAR(25) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see enum table_types:
-- table = 0, view = 1, merge_table = 3, stream = 4 (not used anymore), remote = 5, replica_table = 6
-- Note: values 10, 11, 20 and 30 are synthetically constructed, see
-- view sys.tables. Do not change them as they are used by ODBC
-- SQLTables(SQL_ALL_TABLE_TYPES) and JDBC methods getTableTypes() and
-- getTables()
INSERT INTO sys.table_types (table_type_id, table_type_name) VALUES
  (0, 'TABLE'),
  (1, 'VIEW'),
  (3, 'MERGE TABLE'),
  (5, 'REMOTE TABLE'),
  (6, 'REPLICA TABLE'),
  (7, 'UNLOGGED TABLE'),
-- synthetically constructed system obj variants (added 10 to
-- sys._tables.type value when sys._tables.system is true).
  (10, 'SYSTEM TABLE'),
  (11, 'SYSTEM VIEW'),
-- synthetically constructed temporary variants (added 20 or 30 to
-- sys._tables.type value depending on values of temporary and
-- commit_action).
  (20, 'GLOBAL TEMPORARY TABLE'),
  (30, 'LOCAL TEMPORARY TABLE'),
  (31, 'LOCAL TEMPORARY VIEW');

ALTER TABLE sys.table_types SET READ ONLY;
GRANT SELECT ON sys.table_types TO PUBLIC;


CREATE TABLE sys.function_types (
    function_type_id   SMALLINT NOT NULL PRIMARY KEY,
    function_type_name VARCHAR(30) NOT NULL UNIQUE,
    function_type_keyword VARCHAR(30) NOT NULL);

-- Values taken from sql/include/sql_catalog.h see: #define F_FUNC 1,
-- F_PROC 2, F_AGGR 3, F_FILT 4, F_UNION 5, F_ANALYTIC 6, F_LOADER 7.
INSERT INTO sys.function_types (function_type_id, function_type_name, function_type_keyword) VALUES
  (1, 'Scalar function', 'FUNCTION'),
  (2, 'Procedure', 'PROCEDURE'),
  (3, 'Aggregate function', 'AGGREGATE'),
  (4, 'Filter function', 'FILTER FUNCTION'),
  (5, 'Function returning a table', 'FUNCTION'),
  (6, 'Analytic function', 'WINDOW'),
  (7, 'Loader function', 'LOADER');

ALTER TABLE sys.function_types SET READ ONLY;
GRANT SELECT ON sys.function_types TO PUBLIC;


CREATE TABLE sys.function_languages (
    language_id      SMALLINT    NOT NULL PRIMARY KEY,
    language_name    VARCHAR(20) NOT NULL UNIQUE,
    language_keyword VARCHAR(20));

-- Values taken from sql/include/sql_catalog.h see: #define
-- FUNC_LANG_INT 0, FUNC_LANG_MAL 1, FUNC_LANG_SQL 2, FUNC_LANG_R 3,
-- FUNC_LANG_C 4, FUNC_LANG_PY 6, FUNC_LANG_PY3 10, FUNC_LANG_CPP 12.
INSERT INTO sys.function_languages (language_id, language_name, language_keyword) VALUES
  (0, 'Internal C', NULL),
  (1, 'MAL', NULL),
  (2, 'SQL', NULL),
  (3, 'R', 'R'),
  (4, 'C', 'C'),
--  (5, 'J', 'J'), -- Javascript? not yet available for use
  (6, 'Python', 'PYTHON'),
  (10, 'Python3', 'PYTHON3'),
  (12, 'C++', 'CPP');

ALTER TABLE sys.function_languages SET READ ONLY;
GRANT SELECT ON sys.function_languages TO PUBLIC;


CREATE TABLE sys.key_types (
    key_type_id   SMALLINT NOT NULL PRIMARY KEY,
    key_type_name VARCHAR(35) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see typedef enum
-- key_type: pkey, ukey, fkey, unndkey, ckey.
INSERT INTO sys.key_types (key_type_id, key_type_name) VALUES
  (0, 'Primary Key'),
  (1, 'Unique Key'),
  (2, 'Foreign Key'),
  (3, 'Unique Key With Nulls Not Distinct'),
  (4, 'Check Constraint');

ALTER TABLE sys.key_types SET READ ONLY;
GRANT SELECT ON sys.key_types TO PUBLIC;


CREATE TABLE sys.fkey_actions (
    action_id   SMALLINT NOT NULL PRIMARY KEY,
    action_name VARCHAR(15) NOT NULL);

-- Values taken from sql/include/sql_catalog.h see sql_fkey
-- and sql/server/sql_parser.y  search for: ref_action:
INSERT INTO sys.fkey_actions (action_id, action_name) VALUES
  (0, 'NO ACTION'),
  (1, 'CASCADE'),
  (2, 'RESTRICT'),
  (3, 'SET NULL'),
  (4, 'SET DEFAULT');

ALTER TABLE sys.fkey_actions SET READ ONLY;
GRANT SELECT ON sys.fkey_actions TO PUBLIC;


CREATE VIEW sys.fkeys AS
SELECT id, table_id, type, name, rkey, update_action_id, upd.action_name as update_action, delete_action_id, del.action_name as delete_action FROM (
 SELECT id, table_id, type, name, rkey, cast((("action" >> 8) & 255) as smallint) as update_action_id, cast(("action" & 255) as smallint) AS delete_action_id FROM sys.keys WHERE type = 2
 UNION ALL
 SELECT id, table_id, type, name, rkey, cast((("action" >> 8) & 255) as smallint) as update_action_id, cast(("action" & 255) as smallint) AS delete_action_id FROM tmp.keys WHERE type = 2
) AS fks
JOIN sys.fkey_actions upd ON fks.update_action_id = upd.action_id
JOIN sys.fkey_actions del ON fks.delete_action_id = del.action_id;

GRANT SELECT ON sys.fkeys TO PUBLIC;


CREATE TABLE sys.index_types (
    index_type_id   SMALLINT NOT NULL PRIMARY KEY,
    index_type_name VARCHAR(25) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see typedef enum
-- idx_type: hash_idx, join_idx, oph_idx, no_idx, imprints_idx,
-- ordered_idx.
INSERT INTO sys.index_types (index_type_id, index_type_name) VALUES
  (0, 'Hash'),
  (1, 'Join'),
  (2, 'Order preserving hash'),
  (3, 'No-index'),
  (4, 'Imprint'),
  (5, 'Ordered');

ALTER TABLE sys.index_types SET READ ONLY;
GRANT SELECT ON sys.index_types TO PUBLIC;


CREATE TABLE sys.privilege_codes (
    privilege_code_id   INT NOT NULL PRIMARY KEY,
    privilege_code_name VARCHAR(40) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see: #define
-- PRIV_SELECT 1, PRIV_UPDATE 2, PRIV_INSERT 4, PRIV_DELETE 8,
-- PRIV_EXECUTE 16, PRIV_GRANT 32, PRIV_TRUNCATE 64
INSERT INTO sys.privilege_codes (privilege_code_id, privilege_code_name) VALUES
  (1, 'SELECT'),
  (2, 'UPDATE'),
  (4, 'INSERT'),
  (8, 'DELETE'),
  (16, 'EXECUTE'),
  (32, 'GRANT'),
  (64, 'TRUNCATE'),
-- next are combined privileges applicable only to tables and columns
  (3, 'SELECT,UPDATE'),
  (5, 'SELECT,INSERT'),
  (6, 'INSERT,UPDATE'),
  (7, 'SELECT,INSERT,UPDATE'),
  (9, 'SELECT,DELETE'),
  (10, 'UPDATE,DELETE'),
  (11, 'SELECT,UPDATE,DELETE'),
  (12, 'INSERT,DELETE'),
  (13, 'SELECT,INSERT,DELETE'),
  (14, 'INSERT,UPDATE,DELETE'),
  (15, 'SELECT,INSERT,UPDATE,DELETE'),
  (65, 'SELECT,TRUNCATE'),
  (66, 'UPDATE,TRUNCATE'),
  (67, 'SELECT,UPDATE,TRUNCATE'),
  (68, 'INSERT,TRUNCATE'),
  (69, 'SELECT,INSERT,TRUNCATE'),
  (70, 'INSERT,UPDATE,TRUNCATE'),
  (71, 'SELECT,INSERT,UPDATE,TRUNCATE'),
  (72, 'DELETE,TRUNCATE'),
  (73, 'SELECT,DELETE,TRUNCATE'),
  (74, 'UPDATE,DELETE,TRUNCATE'),
  (75, 'SELECT,UPDATE,DELETE,TRUNCATE'),
  (76, 'INSERT,DELETE,TRUNCATE'),
  (77, 'SELECT,INSERT,DELETE,TRUNCATE'),
  (78, 'INSERT,UPDATE,DELETE,TRUNCATE'),
  (79, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE');

ALTER TABLE sys.privilege_codes SET READ ONLY;
GRANT SELECT ON sys.privilege_codes TO PUBLIC;


-- Utility views to list the defined roles and users.
-- Note: sys.auths contains both users and roles as the names must be distinct.
CREATE VIEW sys.roles AS SELECT id, name, grantor FROM sys.auths a WHERE a.name NOT IN (SELECT u.name FROM sys.db_user_info u);
GRANT SELECT ON sys.roles TO PUBLIC;
CREATE VIEW sys.users AS SELECT name, fullname, default_schema, schema_path, max_memory, max_workers, optimizer, default_role FROM sys.db_user_info;
GRANT SELECT ON sys.users TO PUBLIC;
CREATE FUNCTION sys.db_users() RETURNS TABLE(name VARCHAR(2048)) RETURN SELECT name FROM sys.db_user_info;

-- Utility view to list the standard variables (as defined in sys.var()) and their run-time value
CREATE VIEW sys.var_values (var_name, value) AS
SELECT 'current_role', current_role UNION ALL
SELECT 'current_schema', current_schema UNION ALL
SELECT 'current_timezone', current_timezone UNION ALL
SELECT 'current_user', current_user UNION ALL
SELECT 'debug', debug UNION ALL
SELECT 'last_id', last_id UNION ALL
SELECT 'optimizer', optimizer UNION ALL
SELECT 'pi', pi() UNION ALL
SELECT 'rowcnt', rowcnt;
GRANT SELECT ON sys.var_values TO PUBLIC;
