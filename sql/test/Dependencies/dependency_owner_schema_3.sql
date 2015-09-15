ALTER USER "monet_test" SET SCHEMA "sys";

DROP SCHEMA test;

DROP USER monet_test;


DROP SCHEMA test_2;

--Schema s has a dependency on user u
SELECT s.name, u.name, 'DEP_USER' from schemas as s, users u where u.default_schema = s.id;

--User (owner) has a dependency in schema s
SELECT a.name, s.name, 'DEP_SCHEMA' from schemas as s, auths a where s.owner = a.id;


--Table t has a dependency on view v
SELECT t.name, v.name, 'DEP_VIEW' from tables as t, tables as v, dependencies as dep where t.id = dep.id AND v.id = dep.depend_id AND dep.depend_type = 5 AND v.type in (1, 11, 21, 31);

--Table t has a dependency on index  i
SELECT t.name, i.name, 'DEP_INDEX' from tables as t, idxs as i where i.table_id = t.id and i.name not in (select name from keys) and t.type in (0, 10, 20, 30);

--Table t has a dependency on trigger tri

(SELECT t.name, tri.name, 'DEP_TRIGGER' from tables as t, triggers as tri where tri.table_id = t.id) UNION (SELECT t.name, tri.name, 'DEP_TRIGGER' from triggers tri, tables t, dependencies dep where dep.id = t.id AND dep.depend_id =tri.id AND dep.depend_type = 8);

--Table t has a dependency on foreign key k
SELECT t.name, fk.name, 'DEP_FKEY' from tables as t, keys as k, keys as fk where fk.rkey = k.id and k.table_id = t.id;

--Table t has a dependency on function f
SELECT t.name, f.name, 'DEP_FUNC' from functions as f, tables as t, dependencies as dep where t.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 AND t.type in (0, 10, 20, 30) ORDER BY t.name, f.name;


--Column c has a dependency on view v
SELECT c.name, v.name, 'DEP_VIEW' from columns as c, tables as v, dependencies as dep where c.id = dep.id AND v.id = dep.depend_id AND dep.depend_type = 5 AND v.type in (1, 11, 21, 31);

--Column c has a dependency on key k
SELECT c.name, k.name, 'DEP_KEY' from columns as c, objects as kc, keys as k where kc."name" = c.name AND kc.id = k.id AND k.table_id = c.table_id AND k.rkey = -1;

--Column c has a dependency on index i 
SELECT c.name, i.name, 'DEP_INDEX' from columns as c, objects as kc, idxs as i where kc."name" = c.name AND kc.id = i.id AND c.table_id = i.table_id AND i.name not in (select name from keys);

--Column c has a dependency on function f
SELECT c.name, f.name, 'DEP_FUNC' from functions as f, columns as c, dependencies as dep where c.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 ORDER BY c.name, f.name;

--Column c has a dependency on trigger tri
SELECT c.name, tri.name, 'DEP_TRIGGER' from columns as c, triggers as tri, dependencies as dep where dep.id = c.id AND dep.depend_id =tri.id AND dep.depend_type = 8;


--View v has a dependency on function f
SELECT v.name, f.name, 'DEP_FUNC' from functions as f, tables as v, dependencies as dep where v.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 AND v.type in (1, 11, 21, 31) ORDER BY v.name, f.name;

--View v has a dependency on index i
SELECT v.name, i.name, 'DEP_INDEX' from tables as v, idxs as i where i.table_id = v.id and i.name not in (select name from keys) and v.type in (1, 11, 21, 31);

--View v has a dependency on trigger tri
SELECT v.name, tri.name, 'DEP_TRIGGER' from tables as v, triggers as tri, dependencies as dep where dep.id = v.id AND dep.depend_id =tri.id AND dep.depend_type = 8 AND v.type in (1, 11, 21, 31);


--Functions f1 has a dependency on function f2
SELECT f1.name, f2.name, 'DEP_FUNC' from functions as f1, functions as f2, dependencies as dep where f1.id = dep.id AND f2.id = dep.depend_id AND dep.depend_type = 7 ORDER BY f1.name, f2.name;

--Function f1 has a dependency on trigger tri
SELECT f.name, tri.name, 'DEP_TRIGGER' from functions as f, triggers as tri, dependencies as dep where dep.id = f.id AND dep.depend_id =tri.id AND dep.depend_type = 8;


--Key k has a dependency on foreign key fk
SELECT k.name, fk.name, 'DEP_FKEY' from keys as k, keys as fk where fk.rkey = k.id;
