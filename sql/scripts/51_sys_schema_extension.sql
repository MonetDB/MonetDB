-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

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
  ('AND'),
  ('ANY'),
  ('ASC'),
  ('ASYMMETRIC'),
  ('ATOMIC'),
  ('AUTO_INCREMENT'),
  ('BEFORE'),
  ('BEGIN'),
  ('BEST'),
  ('BETWEEN'),
  ('BIGINT'),
  ('BIGSERIAL'),
  ('BINARY'),
  ('BLOB'),
  ('BY'),
  ('CALL'),
  ('CASCADE'),
  ('CASE'),
  ('CAST'),
  ('CHAIN'),
  ('CHAR'),
  ('CHARACTER'),
  ('CHECK'),
  ('CLOB'),
  ('COALESCE'),
  ('COMMIT'),
  ('COMMITTED'),
  ('CONSTRAINT'),
  ('CONVERT'),
  ('COPY'),
  ('CORRESPONDING'),
  ('CREATE'),
  ('CROSS'),
  ('CURRENT'),
  ('CURRENT_DATE'),
  ('CURRENT_ROLE'),
  ('CURRENT_TIME'),
  ('CURRENT_TIMESTAMP'),
  ('CURRENT_USER'),
  ('DAY'),
  ('DEC'),
  ('DECIMAL'),
  ('DECLARE'),
  ('DEFAULT'),
  ('DELETE'),
  ('DELIMITERS'),
  ('DESC'),
  ('DO'),
  ('DOUBLE'),
  ('DROP'),
  ('EACH'),
  ('EFFORT'),
  ('ELSE'),
  ('ELSEIF'),
  ('ENCRYPTED'),
  ('END'),
  ('ESCAPE'),
  ('EVERY'),
  ('EXCEPT'),
  ('EXCLUDE'),
  ('EXISTS'),
  ('EXTERNAL'),
  ('EXTRACT'),
  ('FALSE'),
  ('FLOAT'),
  ('FOLLOWING'),
  ('FOR'),
  ('FOREIGN'),
  ('FROM'),
  ('FULL'),
  ('FUNCTION'),
  ('GENERATED'),
  ('GLOBAL'),
  ('GRANT'),
  ('GROUP'),
  ('HAVING'),
  ('HOUR'),
  ('HUGEINT'),
  ('IDENTITY'),
  ('IF'),
  ('ILIKE'),
  ('IN'),
  ('INDEX'),
  ('INNER'),
  ('INSERT'),
  ('INT'),
  ('INTEGER'),
  ('INTERSECT'),
  ('INTO'),
  ('IS'),
  ('ISOLATION'),
  ('JOIN'),
  ('LEFT'),
  ('LIKE'),
  ('LIMIT'),
  ('LOCAL'),
  ('LOCALTIME'),
  ('LOCALTIMESTAMP'),
  ('LOCKED'),
  ('MEDIUMINT'),
  ('MERGE'),
  ('MINUTE'),
  ('MONTH'),
  ('NATURAL'),
  ('NEW'),
  ('NEXT'),
  ('NOCYCLE'),
  ('NOMAXVALUE'),
  ('NOMINVALUE'),
  ('NOT'),
  ('NOW'),
  ('NULL'),
  ('NULLIF'),
  ('NUMERIC'),
  ('OF'),
  ('OFFSET'),
  ('OLD'),
  ('ON'),
  ('ONLY'),
  ('OPTION'),
  ('OR'),
  ('ORDER'),
  ('OTHERS'),
  ('OUTER'),
  ('OVER'),
  ('PARTIAL'),
  ('PARTITION'),
  ('POSITION'),
  ('PRECEDING'),
  ('PRESERVE'),
  ('PRIMARY'),
  ('PRIVILEGES'),
  ('PROCEDURE'),
  ('PUBLIC'),
  ('RANGE'),
  ('READ'),
  ('REAL'),
  ('RECORDS'),
  ('REFERENCES'),
  ('REFERENCING'),
  ('REMOTE'),
  ('RENAME'),
  ('REPEATABLE'),
  ('REPLICA'),
  ('RESTART'),
  ('RESTRICT'),
  ('RETURN'),
  ('RETURNS'),
  ('REVOKE'),
  ('RIGHT'),
  ('ROLLBACK'),
  ('ROWS'),
  ('SAMPLE'),
  ('SAVEPOINT'),
  ('SECOND'),
  ('SELECT'),
  ('SEQUENCE'),
  ('SERIAL'),
  ('SERIALIZABLE'),
  ('SESSION_USER'),
  ('SET'),
  ('SIMPLE'),
  ('SMALLINT'),
  ('SOME'),
  ('SPLIT_PART'),
  ('STDIN'),
  ('STDOUT'),
  ('STORAGE'),
  ('STREAM'),
  ('STRING'),
  ('SUBSTRING'),
  ('SYMMETRIC'),
  ('THEN'),
  ('TIES'),
  ('TINYINT'),
  ('TO'),
  ('TRANSACTION'),
  ('TRIGGER'),
  ('TRUE'),
  ('UNBOUNDED'),
  ('UNCOMMITTED'),
  ('UNENCRYPTED'),
  ('UNION'),
  ('UNIQUE'),
  ('UPDATE'),
  ('USER'),
  ('USING'),
  ('VALUES'),
  ('VARCHAR'),
  ('VARYING'),
  ('VIEW'),
  ('WHEN'),
  ('WHERE'),
  ('WHILE'),
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
  ('XMLVALIDATE');

ALTER TABLE sys.keywords SET READ ONLY;
GRANT SELECT ON sys.keywords TO PUBLIC;


CREATE TABLE sys.table_types (
    table_type_id   SMALLINT NOT NULL PRIMARY KEY,
    table_type_name VARCHAR(25) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see enum table_types:
-- table = 0, view = 1, merge_table = 3, stream = 4, remote = 5,
-- replica_table = 6.
-- Note: values 10, 11, 20 and 30 are synthetically constructed, see
-- view sys.tables. Do not change them as they are used by ODBC
-- SQLTables(SQL_ALL_TABLE_TYPES) and JDBC methods getTableTypes() and
-- getTables()
INSERT INTO sys.table_types (table_type_id, table_type_name) VALUES
  (0, 'TABLE'),
  (1, 'VIEW'),
  (3, 'MERGE TABLE'),
  (4, 'STREAM TABLE'),
  (5, 'REMOTE TABLE'),
  (6, 'REPLICA TABLE'),
-- synthetically constructed system obj variants (added 10 to
-- sys._tables.type value when sys._tables.system is true).
  (10, 'SYSTEM TABLE'),
  (11, 'SYSTEM VIEW'),
-- synthetically constructed temporary variants (added 20 or 30 to
-- sys._tables.type value depending on values of temporary and
-- commit_action).
  (20, 'GLOBAL TEMPORARY TABLE'),
  (30, 'LOCAL TEMPORARY TABLE');

ALTER TABLE sys.table_types SET READ ONLY;
GRANT SELECT ON sys.table_types TO PUBLIC;


CREATE TABLE sys.dependency_types (
    dependency_type_id   SMALLINT NOT NULL PRIMARY KEY,
    dependency_type_name VARCHAR(15) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h  see: #define SCHEMA_DEPENDENCY 1, TABLE_DEPENDENCY 2, ..., TYPE_DEPENDENCY 15.
INSERT INTO sys.dependency_types (dependency_type_id, dependency_type_name) VALUES
  (1, 'SCHEMA'),
  (2, 'TABLE'),
  (3, 'COLUMN'),
  (4, 'KEY'),
  (5, 'VIEW'),
  (6, 'USER'),
  (7, 'FUNCTION'),
  (8, 'TRIGGER'),
  (9, 'OWNER'),
  (10, 'INDEX'),
  (11, 'FKEY'),
  (12, 'SEQUENCE'),
  (13, 'PROCEDURE'),
  (14, 'BE_DROPPED'),
  (15, 'TYPE');

ALTER TABLE sys.dependency_types SET READ ONLY;
GRANT SELECT ON sys.dependency_types TO PUBLIC;


CREATE TABLE sys.function_types (
    function_type_id   SMALLINT NOT NULL PRIMARY KEY,
    function_type_name VARCHAR(30) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see: #define F_FUNC 1,
-- F_PROC 2, F_AGGR 3, F_FILT 4, F_UNION 5, F_ANALYTIC 6, F_LOADER 7.
INSERT INTO sys.function_types (function_type_id, function_type_name) VALUES
  (1, 'Scalar function'),
  (2, 'Procedure'),
  (3, 'Aggregate function'),
  (4, 'Filter function'),
  (5, 'Function returning a table'),
  (6, 'Analytic function'),
  (7, 'Loader function');

ALTER TABLE sys.function_types SET READ ONLY;
GRANT SELECT ON sys.function_types TO PUBLIC;


CREATE TABLE sys.function_languages (
    language_id   SMALLINT NOT NULL PRIMARY KEY,
    language_name VARCHAR(20) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see: #define
-- FUNC_LANG_INT 0, FUNC_LANG_MAL 1, FUNC_LANG_SQL 2, FUNC_LANG_R 3,
-- FUNC_LANG_PY 6, FUNC_LANG_MAP_PY 7, FUNC_LANG_PY2 8,
-- FUNC_LANG_MAP_PY2 9, FUNC_LANG_PY3 10, FUNC_LANG_MAP_PY3 11.
INSERT INTO sys.function_languages (language_id, language_name) VALUES
  (0, 'Internal C'),
  (1, 'MAL'),
  (2, 'SQL'),
  (3, 'R'),
  (6, 'Python'),
  (7, 'Python Mapped'),
  (8, 'Python2'),
  (9, 'Python2 Mapped'),
  (10, 'Python3'),
  (11, 'Python3 Mapped');

ALTER TABLE sys.function_languages SET READ ONLY;
GRANT SELECT ON sys.function_languages TO PUBLIC;


CREATE TABLE sys.key_types (
    key_type_id   SMALLINT NOT NULL PRIMARY KEY,
    key_type_name VARCHAR(15) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see typedef enum
-- key_type: pkey, ukey, fkey.
INSERT INTO sys.key_types (key_type_id, key_type_name) VALUES
  (0, 'Primary Key'),
  (1, 'Unique Key'),
  (2, 'Foreign Key');

ALTER TABLE sys.key_types SET READ ONLY;
GRANT SELECT ON sys.key_types TO PUBLIC;


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
    privilege_code_name VARCHAR(30) NOT NULL UNIQUE);

-- Values taken from sql/include/sql_catalog.h see: #define
-- PRIV_SELECT 1, PRIV_UPDATE 2, PRIV_INSERT 4, PRIV_DELETE 8,
-- PRIV_EXECUTE 16, PRIV_GRANT 32
INSERT INTO sys.privilege_codes (privilege_code_id, privilege_code_name) VALUES
  (1, 'SELECT'),
  (2, 'UPDATE'),
  (4, 'INSERT'),
  (8, 'DELETE'),
  (16, 'EXECUTE'),
  (32, 'GRANT'),
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
  (15, 'SELECT,INSERT,UPDATE,DELETE');

ALTER TABLE sys.privilege_codes SET READ ONLY;
GRANT SELECT ON sys.privilege_codes TO PUBLIC;

