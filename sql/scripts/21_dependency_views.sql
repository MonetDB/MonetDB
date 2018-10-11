-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

-- Add system views for showing dependencies between specific database objects.
-- These system views improve, extend and replace the dependencies_X_on_Y() functions as previously defined in 21_dependency_functions.sql

-- Utility view to combine and list all potential referenced object ids, their name, schema_id (if applicable),
-- table_id (if applicable), table_name (if applicable), obj_type and system table name (which can be used to query the details of the object)
CREATE VIEW sys.ids (id, name, schema_id, table_id, table_name, obj_type, sys_table) AS
SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'author' AS obj_type, 'sys.auths' AS sys_table FROM sys.auths UNION ALL
SELECT id, name, cast(null as int) as schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'schema', 'sys.schemas' FROM sys.schemas UNION ALL
SELECT id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'sys._tables' FROM sys._tables UNION ALL
SELECT id, name, schema_id, id as table_id, name as table_name, case when type = 1 then 'view' else 'table' end, 'tmp._tables' FROM tmp._tables UNION ALL
SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'sys._columns' FROM sys._columns c JOIN sys._tables t ON c.table_id = t.id UNION ALL
SELECT c.id, c.name, t.schema_id, c.table_id, t.name as table_name, 'column', 'tmp._columns' FROM tmp._columns c JOIN tmp._tables t ON c.table_id = t.id UNION ALL
SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'sys.keys' FROM sys.keys k JOIN sys._tables t ON k.table_id = t.id UNION ALL
SELECT k.id, k.name, t.schema_id, k.table_id, t.name as table_name, 'key', 'tmp.keys' FROM tmp.keys k JOIN tmp._tables t ON k.table_id = t.id UNION ALL
SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'sys.idxs' FROM sys.idxs i JOIN sys._tables t ON i.table_id = t.id UNION ALL
SELECT i.id, i.name, t.schema_id, i.table_id, t.name as table_name, 'index', 'tmp.idxs' FROM tmp.idxs i JOIN tmp._tables t ON i.table_id = t.id UNION ALL
SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'sys.triggers' FROM sys.triggers g JOIN sys._tables t ON g.table_id = t.id UNION ALL
SELECT g.id, g.name, t.schema_id, g.table_id, t.name as table_name, 'trigger', 'tmp.triggers' FROM tmp.triggers g JOIN tmp._tables t ON g.table_id = t.id UNION ALL
SELECT id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when type = 2 then 'procedure' else 'function' end, 'sys.functions' FROM sys.functions UNION ALL
SELECT a.id, a.name, f.schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, case when f.type = 2 then 'procedure arg' else 'function arg' end, 'sys.args' FROM sys.args a JOIN sys.functions f ON a.func_id = f.id UNION ALL
SELECT id, name, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'sequence', 'sys.sequences' FROM sys.sequences UNION ALL
SELECT id, sqlname, schema_id, cast(null as int) as table_id, cast(null as varchar(124)) as table_name, 'type', 'sys.types' FROM sys.types WHERE id > 2000 /* exclude system types to prevent duplicates with auths.id */
 ORDER BY id;
/* do not include: SELECT id, 'object', name FROM sys.objects; as it has duplicates with keys, columns, etc */
/* do not include: SELECT id, 'object', name FROM tmp.objects; as it has duplicates with keys, columns, etc */

GRANT SELECT ON sys.ids TO PUBLIC;


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


-- A utility view to enrich the base sys.dependencies table with additional information
-- The sys.ids view is used to join with the table sys.dependencies on field id and on depend_id.
CREATE VIEW sys.dependencies_vw AS
SELECT d.id, i1.obj_type, i1.name,
       d.depend_id as used_by_id, i2.obj_type as used_by_obj_type, i2.name as used_by_name,
       d.depend_type, dt.dependency_type_name
  FROM sys.dependencies d
  JOIN sys.ids i1 ON d.id = i1.id
  JOIN sys.ids i2 ON d.depend_id = i2.id
  JOIN sys.dependency_types dt ON d.depend_type = dt.dependency_type_id
 ORDER BY id, depend_id;

GRANT SELECT ON sys.dependencies_vw TO PUBLIC;


-- **** dependency_type 1 = SCHEMA ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 1;

-- User (owner) has a dependency in schema s.dependencies_owners_on_schemas
CREATE VIEW sys.dependency_owners_on_schemas AS
SELECT a.name AS owner_name, s.id AS schema_id, s.name AS schema_name, CAST(1 AS smallint) AS depend_type
  FROM sys.schemas AS s, sys.auths AS a
 WHERE s.owner = a.id
 ORDER BY a.name, s.name;

GRANT SELECT ON sys.dependency_owners_on_schemas TO PUBLIC;


-- **** dependency_type 2 = TABLE ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 2;


-- **** dependency_type 3 = COLUMN ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 3;


-- **** dependency_type 4 = (P/U)KEY ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 4;
-- Column c is used by key k.
-- Note we use sys.objects instead of sys.dependencies as sys.objects also includes the dependencies on columns used in Unique constraints (keys.type = 1) besides PKey (keys.type = 0)
CREATE VIEW sys.dependency_columns_on_keys AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, c.id AS column_id, c.name AS column_name, k.id AS key_id, k.name AS key_name, CAST(kc.nr +1 AS int) AS key_col_nr, CAST(k.type AS smallint) AS key_type, CAST(4 AS smallint) AS depend_type
  FROM sys.columns AS c, sys.objects AS kc, sys.keys AS k, sys.tables AS t
 WHERE k.table_id = c.table_id AND c.table_id = t.id AND kc.id = k.id AND kc.name = c.name
   AND k.type IN (0, 1)
 ORDER BY t.schema_id, t.name, c.name, k.type, k.name, kc.nr;

GRANT SELECT ON sys.dependency_columns_on_keys TO PUBLIC;


-- **** dependency_type 5 = VIEW ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 5;
-- Table t is used by view v.  table_type 1 = VIEW, 11 = SYSTEM VIEW.
CREATE VIEW sys.dependency_tables_on_views AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, dep.depend_type AS depend_type
  FROM sys.tables AS t, sys.tables AS v, sys.dependencies AS dep
 WHERE t.id = dep.id AND v.id = dep.depend_id
   AND dep.depend_type = 5 AND t.type NOT IN (1, 11) AND v.type IN (1, 11)
 ORDER BY t.schema_id, t.name, v.schema_id, v.name;

GRANT SELECT ON sys.dependency_tables_on_views TO PUBLIC;

-- View v1 has a dependency on view v2.  table_type 1 = VIEW, 11 = SYSTEM VIEW.
CREATE VIEW sys.dependency_views_on_views AS
SELECT v1.schema_id AS view1_schema_id, v1.id AS view1_id, v1.name AS view1_name, v2.schema_id AS view2_schema_id, v2.id AS view2_id, v2.name AS view2_name, dep.depend_type AS depend_type
  FROM sys.tables AS v1, sys.tables AS v2, sys.dependencies AS dep
 WHERE v1.id = dep.id AND v2.id = dep.depend_id
   AND dep.depend_type = 5 AND v1.type IN (1, 11) AND v2.type IN (1, 11)
 ORDER BY v1.schema_id, v1.name, v2.schema_id, v2.name;

GRANT SELECT ON sys.dependency_views_on_views TO PUBLIC;

-- Column c has a dependency on view v.  table_type 1 = VIEW, 11 = SYSTEM VIEW.
CREATE VIEW sys.dependency_columns_on_views AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, c.id AS column_id, c.name AS column_name, v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, dep.depend_type AS depend_type
  FROM sys.columns AS c, sys.tables AS v, sys.tables AS t, sys.dependencies AS dep
 WHERE c.id = dep.id AND v.id = dep.depend_id AND c.table_id = t.id
   AND dep.depend_type = 5 AND v.type IN (1, 11)
 ORDER BY t.schema_id, t.name, c.name, v.name;

GRANT SELECT ON sys.dependency_columns_on_views TO PUBLIC;

-- Function/procedure f has a dependency on view v.
CREATE VIEW sys.dependency_functions_on_views AS
SELECT f.schema_id AS function_schema_id, f.id AS function_id, f.name AS function_name, v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, dep.depend_type AS depend_type
  FROM sys.functions AS f, sys.tables AS v, sys.dependencies AS dep
 WHERE f.id = dep.id AND v.id = dep.depend_id
   AND dep.depend_type = 5 AND v.type IN (1, 11)
 ORDER BY f.schema_id, f.name, v.schema_id, v.name;

GRANT SELECT ON sys.dependency_functions_on_views TO PUBLIC;


-- **** dependency_type 6 = USER ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 6;
-- Schema s has a dependency on user u.
CREATE VIEW sys.dependency_schemas_on_users AS
SELECT s.id AS schema_id, s.name AS schema_name, u.name AS user_name, CAST(6 AS smallint) AS depend_type
  FROM sys.users AS u, sys.schemas AS s
 WHERE u.default_schema = s.id
 ORDER BY s.name, u.name;

GRANT SELECT ON sys.dependency_schemas_on_users TO PUBLIC;

-- **** dependency_type 7 = FUNCTION ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 7 ORDER BY obj_type, id;
-- Table t has a dependency on function f.
CREATE VIEW sys.dependency_tables_on_functions AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type
  FROM sys.functions AS f, sys.tables AS t, sys.dependencies AS dep
 WHERE t.id = dep.id AND f.id = dep.depend_id
   AND dep.depend_type = 7 AND f.type <> 2 AND t.type NOT IN (1, 11)
 ORDER BY t.name, t.schema_id, f.name, f.id;

GRANT SELECT ON sys.dependency_tables_on_functions TO PUBLIC;

-- View v has a dependency on function f.
CREATE VIEW sys.dependency_views_on_functions AS
SELECT v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type
  FROM sys.functions AS f, sys.tables AS v, sys.dependencies AS dep
 WHERE v.id = dep.id AND f.id = dep.depend_id
   AND dep.depend_type = 7 AND f.type <> 2 AND v.type IN (1, 11)
 ORDER BY v.name, v.schema_id, f.name, f.id;

GRANT SELECT ON sys.dependency_views_on_functions TO PUBLIC;

-- Column c has a dependency on function f.
CREATE VIEW sys.dependency_columns_on_functions AS
SELECT c.table_id, c.id AS column_id, c.name, f.id AS function_id, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type
  FROM sys.functions AS f, sys.columns AS c, sys.dependencies AS dep
 WHERE c.id = dep.id AND f.id = dep.depend_id
   AND dep.depend_type = 7 AND f.type <> 2
 ORDER BY c.name, c.table_id, f.name, f.id;

GRANT SELECT ON sys.dependency_columns_on_functions TO PUBLIC;

-- Function f1 has a dependency on function f2.
CREATE VIEW sys.dependency_functions_on_functions AS
SELECT f1.schema_id, f1.id AS function_id, f1.name AS function_name, f1.type AS function_type,
       f2.schema_id AS used_in_function_schema_id, f2.id AS used_in_function_id, f2.name AS used_in_function_name, f2.type AS used_in_function_type, dep.depend_type AS depend_type
  FROM sys.functions AS f1, sys.functions AS f2, sys.dependencies AS dep
 WHERE f1.id = dep.id AND f2.id = dep.depend_id
   AND dep.depend_type = 7 AND f2.type <> 2
 ORDER BY f1.name, f1.id, f2.name, f2.id;

GRANT SELECT ON sys.dependency_functions_on_functions TO PUBLIC;


-- **** dependency_type 8 = TRIGGER ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 8 ORDER BY obj_type, id;
-- Table t has a dependency on trigger tri.
CREATE VIEW sys.dependency_tables_on_triggers AS
(SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, tri.id AS trigger_id, tri.name AS trigger_name, CAST(8 AS smallint) AS depend_type
  FROM sys.tables AS t, sys.triggers AS tri
 WHERE tri.table_id = t.id)
UNION
(SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, tri.id AS trigger_id, tri.name AS trigger_name, dep.depend_type AS depend_type
  FROM sys.tables AS t, sys.triggers AS tri, sys.dependencies AS dep
 WHERE dep.id = t.id AND dep.depend_id = tri.id
   AND dep.depend_type = 8)
 ORDER BY table_schema_id, table_name, trigger_name;

GRANT SELECT ON sys.dependency_tables_on_triggers TO PUBLIC;

-- Column c has a dependency on trigger tri.
CREATE VIEW sys.dependency_columns_on_triggers AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, tri.id AS trigger_id, tri.name AS trigger_name, c.id AS column_id, c.name AS column_name, dep.depend_type AS depend_type
  FROM sys.tables AS t, sys.columns AS c, sys.triggers AS tri, sys.dependencies AS dep
 WHERE dep.id = c.id AND dep.depend_id = tri.id AND c.table_id = t.id
   AND dep.depend_type = 8
 ORDER BY t.schema_id, t.name, tri.name, c.name;

GRANT SELECT ON sys.dependency_columns_on_triggers TO PUBLIC;

-- Function f has a dependency on trigger tri.
CREATE VIEW sys.dependency_functions_on_triggers AS
SELECT f.schema_id AS function_schema_id, f.id AS function_id, f.name AS function_name, f.type AS function_type,
       tri.id AS trigger_id, tri.name AS trigger_name, tri.table_id AS trigger_table_id, dep.depend_type AS depend_type
  FROM sys.functions AS f, sys.triggers AS tri, sys.dependencies AS dep
 WHERE dep.id = f.id AND dep.depend_id = tri.id
   AND dep.depend_type = 8
 ORDER BY f.schema_id, f.name, tri.name;

GRANT SELECT ON sys.dependency_functions_on_triggers TO PUBLIC;


-- **** dependency_type 9 = OWNER ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 9;


-- **** dependency_type 10 = INDEX ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 10 ORDER BY obj_type, id;
-- Table t has a dependency on user created secondary index i.
CREATE VIEW sys.dependency_tables_on_indexes AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, i.id AS index_id, i.name AS index_name, i.type AS index_type, CAST(10 AS smallint) AS depend_type
  FROM sys.tables AS t, sys.idxs AS i
 WHERE i.table_id = t.id
    -- exclude internal system generated and managed indexes for enforcing declarative PKey and Unique constraints
   AND (i.table_id, i.name) NOT IN (SELECT k.table_id, k.name FROM sys.keys k)
 ORDER BY t.schema_id, t.name, i.name;

GRANT SELECT ON sys.dependency_tables_on_indexes TO PUBLIC;

-- Column c has a dependency on index i.
CREATE VIEW sys.dependency_columns_on_indexes AS
SELECT c.id AS column_id, c.name AS column_name, t.id AS table_id, t.name AS table_name, t.schema_id, i.id AS index_id, i.name AS index_name, i.type AS index_type, CAST(ic.nr +1 AS INT) AS seq_nr, CAST(10 AS smallint) AS depend_type
  FROM sys.tables AS t, sys.columns AS c, sys.objects AS ic, sys.idxs AS i
 WHERE ic.name = c.name AND ic.id = i.id AND c.table_id = i.table_id AND c.table_id = t.id
    -- exclude internal system generated and managed indexes for enforcing declarative PKey and Unique constraints
   AND (i.table_id, i.name) NOT IN (SELECT k.table_id, k.name FROM sys.keys k)
 ORDER BY c.name, t.name, t.schema_id, i.name, ic.nr;

GRANT SELECT ON sys.dependency_columns_on_indexes TO PUBLIC;


-- **** dependency_type 11 = FKEY ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 11 ORDER BY obj_type, id;
-- Table t has a dependency on foreign key k.
CREATE VIEW sys.dependency_tables_on_foreignkeys AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, fk.name AS fk_name, CAST(k.type AS smallint) AS key_type, CAST(11 AS smallint) AS depend_type
  FROM sys.tables AS t, sys.keys AS k, sys.keys AS fk
 WHERE fk.rkey = k.id and k.table_id = t.id
 ORDER BY t.schema_id, t.name, fk.name;

GRANT SELECT ON sys.dependency_tables_on_foreignkeys TO PUBLIC;

-- Key k has a dependency on foreign key fk.
CREATE VIEW sys.dependency_keys_on_foreignkeys AS
SELECT k.table_id AS key_table_id, k.id AS key_id, k.name AS key_name, fk.table_id AS fk_table_id, fk.id AS fk_id, fk.name AS fk_name, CAST(k.type AS smallint) AS key_type, CAST(11 AS smallint) AS depend_type
  FROM sys.keys AS k, sys.keys AS fk
 WHERE k.id = fk.rkey
 ORDER BY k.name, fk.name;

GRANT SELECT ON sys.dependency_keys_on_foreignkeys TO PUBLIC;


-- **** dependency_type 12 = SEQUENCE ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 12;


-- **** dependency_type 13 = PROCEDURE ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 13 ORDER BY obj_type, id;
-- Table t has a dependency on procedure p.
CREATE VIEW sys.dependency_tables_on_procedures AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type
  FROM sys.functions AS p, sys.tables AS t, sys.dependencies AS dep
 WHERE t.id = dep.id AND p.id = dep.depend_id
   AND dep.depend_type = 13 AND p.type = 2 AND t.type NOT IN (1, 11)
 ORDER BY t.name, t.schema_id, p.name, p.id;

GRANT SELECT ON sys.dependency_tables_on_procedures TO PUBLIC;

-- View v has a dependency on procedure p.
CREATE VIEW sys.dependency_views_on_procedures AS
SELECT v.schema_id AS view_schema_id, v.id AS view_id, v.name AS view_name, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type
  FROM sys.functions AS p, sys.tables AS v, sys.dependencies AS dep
 WHERE v.id = dep.id AND p.id = dep.depend_id
   AND dep.depend_type = 13 AND p.type = 2 AND v.type IN (1, 11)
 ORDER BY v.name, v.schema_id, p.name, p.id;

GRANT SELECT ON sys.dependency_views_on_procedures TO PUBLIC;

-- Column c has a dependency on procedure p.
CREATE VIEW sys.dependency_columns_on_procedures AS
SELECT c.table_id, c.id AS column_id, c.name AS column_name, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type
  FROM sys.functions AS p, sys.columns AS c, sys.dependencies AS dep
 WHERE c.id = dep.id AND p.id = dep.depend_id
   AND dep.depend_type = 13 AND p.type = 2
 ORDER BY c.name, c.table_id, p.name, p.id;

GRANT SELECT ON sys.dependency_columns_on_procedures TO PUBLIC;

-- Function f has a dependency on procedure p.
CREATE VIEW sys.dependency_functions_on_procedures AS
SELECT f.schema_id AS function_schema_id, f.id AS function_id, f.name AS function_name, f.type AS function_type,
       p.schema_id AS procedure_schema_id, p.id AS procedure_id, p.name AS procedure_name, p.type AS procedure_type, dep.depend_type AS depend_type
  FROM sys.functions AS p, sys.functions AS f, sys.dependencies AS dep
 WHERE f.id = dep.id AND p.id = dep.depend_id
   AND dep.depend_type = 13 AND p.type = 2
 ORDER BY p.name, p.id, f.name, f.id;

GRANT SELECT ON sys.dependency_functions_on_procedures TO PUBLIC;


-- **** dependency_type 14 = BE_DROPPED ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 14;


-- **** dependency_type 15 = TYPE ***
-- SELECT * FROM sys.dependencies_vw WHERE depend_type = 15 ORDER BY obj_type, id;
-- Column c depends on type dt
CREATE VIEW sys.dependency_columns_on_types AS
SELECT t.schema_id AS table_schema_id, t.id AS table_id, t.name AS table_name, dt.id AS type_id, dt.sqlname AS type_name, c.id AS column_id, c.name AS column_name, dep.depend_type AS depend_type
  FROM sys.tables AS t, sys.columns AS c, sys.types AS dt, sys.dependencies AS dep
 WHERE dep.id = dt.id AND dep.depend_id = c.id AND c.table_id = t.id
   AND dep.depend_type = 15
 ORDER BY dt.sqlname, t.name, c.name, c.id;

GRANT SELECT ON sys.dependency_columns_on_types TO PUBLIC;

-- Function f depends on type dt
CREATE VIEW sys.dependency_functions_on_types AS
SELECT dt.id AS type_id, dt.sqlname AS type_name, f.id AS function_id, f.name AS function_name, f.type AS function_type, dep.depend_type AS depend_type
  FROM sys.functions AS f, sys.types AS dt, sys.dependencies AS dep
 WHERE dep.id = dt.id AND dep.depend_id = f.id
   AND dep.depend_type = 15
 ORDER BY dt.sqlname, f.name, f.id;

GRANT SELECT ON sys.dependency_functions_on_types TO PUBLIC;

-- Arg c depends on type dt
CREATE VIEW sys.dependency_args_on_types AS
SELECT dt.id AS type_id, dt.sqlname AS type_name, f.id AS function_id, f.name AS function_name, a.id AS arg_id, a.name AS arg_name, a.number AS arg_nr, dep.depend_type AS depend_type
  FROM sys.args AS a, sys.functions AS f, sys.types AS dt, sys.dependencies AS dep
 WHERE dep.id = dt.id AND dep.depend_id = a.id AND a.func_id = f.id
   AND dep.depend_type = 15
 ORDER BY dt.sqlname, f.name, a.number, a.name;

GRANT SELECT ON sys.dependency_args_on_types TO PUBLIC;

