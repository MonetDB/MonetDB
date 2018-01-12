-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0.  If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.
--
-- Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.

--Schema s has a dependency on user u
CREATE FUNCTION dependencies_schemas_on_users()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT s.name, u.name, 'DEP_USER' from schemas as s, users u where u.default_schema = s.id);

--User (owner) has a dependency in schema s
CREATE FUNCTION dependencies_owners_on_schemas()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT a.name, s.name, 'DEP_SCHEMA' from schemas as s, auths a where s.owner = a.id);


--Table t has a dependency on view v
CREATE FUNCTION dependencies_tables_on_views()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT t.name, v.name, 'DEP_VIEW' from tables as t, tables as v, dependencies as dep where t.id = dep.id AND v.id = dep.depend_id AND dep.depend_type = 5 AND v.type = 1);

--Table t has a dependency on index  i
CREATE FUNCTION dependencies_tables_on_indexes()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT t.name, i.name, 'DEP_INDEX' from tables as t, idxs as i where i.table_id = t.id and i.name not in (select name from keys) and t.type = 0);

--Table t has a dependency on trigger tri

CREATE FUNCTION dependencies_tables_on_triggers()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE ((SELECT t.name, tri.name, 'DEP_TRIGGER' from tables as t, triggers as tri where tri.table_id = t.id) UNION (SELECT t.name, tri.name, 'DEP_TRIGGER' from triggers tri, tables t, dependencies dep where dep.id = t.id AND dep.depend_id =tri.id AND dep.depend_type = 8));

--Table t has a dependency on foreign key k
CREATE FUNCTION dependencies_tables_on_foreignKeys()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT t.name, fk.name, 'DEP_FKEY' from tables as t, keys as k, keys as fk where fk.rkey = k.id and k.table_id = t.id);

--Table t has a dependency on function f
CREATE FUNCTION dependencies_tables_on_functions()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT t.name, f.name, 'DEP_FUNC' from functions as f, tables as t, dependencies as dep where t.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 AND t.type = 0);


--Column c has a dependency on view v
CREATE FUNCTION dependencies_columns_on_views()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT c.name, v.name, 'DEP_VIEW' from columns as c, tables as v, dependencies as dep where c.id = dep.id AND v.id = dep.depend_id AND dep.depend_type = 5 AND v.type = 1);

--Column c has a dependency on key k
CREATE FUNCTION dependencies_columns_on_keys()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT c.name, k.name, 'DEP_KEY' from columns as c, objects as kc, keys as k where kc."name" = c.name AND kc.id = k.id AND k.table_id = c.table_id AND k.rkey = -1);

--Column c has a dependency on index i
CREATE FUNCTION dependencies_columns_on_indexes()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT c.name, i.name, 'DEP_INDEX' from columns as c, objects as kc, idxs as i where kc."name" = c.name AND kc.id = i.id AND c.table_id = i.table_id AND i.name not in (select name from keys));

--Column c has a dependency on function f
CREATE FUNCTION dependencies_columns_on_functions()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT c.name, f.name, 'DEP_FUNC' from functions as f, columns as c, dependencies as dep where c.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7);

--Column c has a dependency on trigger tri
CREATE FUNCTION dependencies_columns_on_triggers()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT c.name, tri.name, 'DEP_TRIGGER' from columns as c, triggers as tri, dependencies as dep where dep.id = c.id AND dep.depend_id =tri.id AND dep.depend_type = 8);


--View v has a dependency on function f
CREATE FUNCTION dependencies_views_on_functions()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT v.name, f.name, 'DEP_FUNC' from functions as f, tables as v, dependencies as dep where v.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 AND v.type = 1);

--View v has a dependency on trigger tri
CREATE FUNCTION dependencies_views_on_triggers()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT v.name, tri.name, 'DEP_TRIGGER' from tables as v, triggers as tri, dependencies as dep where dep.id = v.id AND dep.depend_id =tri.id AND dep.depend_type = 8 AND v.type = 1);


--Function f1 has a dependency on function f2
CREATE FUNCTION dependencies_functions_on_functions()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT f1.name, f2.name, 'DEP_FUNC' from functions as f1, functions as f2, dependencies as dep where f1.id = dep.id AND f2.id = dep.depend_id AND dep.depend_type = 7);

--Function f1 has a dependency on trigger tri
CREATE FUNCTION dependencies_functions_os_triggers()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT f.name, tri.name, 'DEP_TRIGGER' from functions as f, triggers as tri, dependencies as dep where dep.id = f.id AND dep.depend_id =tri.id AND dep.depend_type = 8);


--Key k has a dependency on foreign key fk
CREATE FUNCTION dependencies_keys_on_foreignKeys()
RETURNS TABLE (sch varchar(100), usr varchar(100), dep_type varchar(32))
RETURN TABLE (SELECT k.name, fk.name, 'DEP_FKEY' from keys as k, keys as fk where fk.rkey = k.id);



