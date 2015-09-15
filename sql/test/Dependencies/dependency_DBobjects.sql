CREATE TABLE t1(id int, name varchar(1024), age int, PRIMARY KEY(id));

CREATE VIEW v1 as select id, age from t1 where name like 'monet%';

CREATE VIEW v2 as select * from v1;


CREATE FUNCTION f1(num int)
RETURNS int
BEGIN 
	IF num >0
		THEN RETURN 1;
		ELSE RETURN 0;
	END IF;
END;

CREATE FUNCTION f2()
RETURNS TABLE(id int)
BEGIN
	DECLARE TABLE f1_t1(id int);
	DECLARE x int;
	
	SET x = 3;	

	IF f1(x) > 0
		THEN RETURN f1_t1;
	END IF;

	INSERT INTO f1_t1 VALUES(1);

	IF f1(x) < 0
		THEN RETURN f1_t1;
	END IF;

	RETURN TABLE (SELECT t1.id FROM v1, t1 WHERE v1.age > 10 AND t1.name LIKE 'monet');
END;

CREATE INDEX id_index ON t1(id);

CREATE INDEX id_age_index ON v1(id,age);

ALTER TABLE t1 DROP COLUMN id;

CREATE TABLE t2 (id_t1 int, age_v1 int);

ALTER TABLE t2 ADD FOREIGN KEY(id_t1) REFERENCES t1(id);

ALTER TABLE v1 DROP COLUMN age;

ALTER TABLE t2 ADD FOREIGN KEY(age_v1) REFERENCES v1(age);

CREATE TRIGGER trigger_test AFTER INSERT ON t1
	INSERT INTO t2 values(1,23);


--CREATE TRIGGER trigger_test_3 AFTER INSERT ON t1
--	INSERT INTO t1 values(1, 'monetdb', 23);

CREATE TABLE t3 (id int);

CREATE TRIGGER trigger_test_4 AFTER INSERT ON t1
	INSERT INTO t3 values(1);

--Schema s has a dependency on user u
SELECT s.name, u.name, 'DEP_USER' from schemas as s, users u where u.default_schema = s.id;

--User (owner) has a dependency in schema s
SELECT a.name, s.name, 'DEP_SCHEMA' from schemas as s, auths a where s.owner = a.id;


--Table t has a dependency on view v
SELECT t.name, v.name, 'DEP_VIEW' from tables as t, tables as v, dependencies as dep where t.id = dep.id AND v.id = dep.depend_id AND dep.depend_type = 5 AND v.type = 1;

--Table t has a dependency on index  i
SELECT t.name, i.name, 'DEP_INDEX' from tables as t, idxs as i where i.table_id = t.id and i.name not in (select name from keys) and t.type = 0;

--Table t has a dependency on trigger tri
(SELECT t.name as name, tri.name as trigname, 'DEP_TRIGGER' from tables as t, triggers as tri where tri.table_id = t.id) UNION (SELECT t.name as name, tri.name as trigname, 'DEP_TRIGGER' from triggers tri, tables t, dependencies dep where dep.id = t.id AND dep.depend_id =tri.id AND dep.depend_type = 8);

--Table t has a dependency on foreign key k
SELECT t.name, fk.name, 'DEP_FKEY' from tables as t, keys as k, keys as fk where fk.rkey = k.id and k.table_id = t.id;

--Table or System Table t has a dependency on function f
SELECT t.name, f.name, 'DEP_FUNC' from functions as f, tables as t, dependencies as dep where t.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 AND t.type IN (0, 10) ORDER BY t.name, f.name;


--Column c has a dependency on view v
SELECT c.name, v.name, 'DEP_VIEW' from columns as c, tables as v, dependencies as dep where c.id = dep.id AND v.id = dep.depend_id AND dep.depend_type = 5 AND v.type = 1;

--Column c has a dependency on key k
SELECT c.name, k.name, 'DEP_KEY' from columns as c, objects as kc, keys as k where kc."name" = c.name AND kc.id = k.id AND k.table_id = c.table_id AND k.rkey = -1;

--Column c has a dependency on index i 
SELECT c.name, i.name, 'DEP_INDEX' from columns as c, objects as kc, idxs as i where kc."name" = c.name AND kc.id = i.id AND c.table_id = i.table_id AND i.name not in (select name from keys);

--Column c has a dependency on function f
SELECT c.name, f.name, 'DEP_FUNC' from functions as f, columns as c, dependencies as dep where c.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 ORDER BY c.name, f.name;

--Column c has a dependency on trigger tri
SELECT c.name, tri.name, 'DEP_TRIGGER' from columns as c, triggers as tri, dependencies as dep where dep.id = c.id AND dep.depend_id =tri.id AND dep.depend_type = 8;


--View or System View v has a dependency on function f
SELECT v.name, f.name, 'DEP_FUNC' from functions as f, tables as v, dependencies as dep where v.id = dep.id AND f.id = dep.depend_id AND dep.depend_type = 7 AND v.type IN (1, 11) ORDER BY v.name, f.name;

--View v has a dependency on index i
SELECT v.name, i.name, 'DEP_INDEX' from tables as v, idxs as i where i.table_id = v.id and i.name not in (select name from keys) and v.type = 1;

--View v has a dependency on trigger tri
SELECT v.name, tri.name, 'DEP_TRIGGER' from tables as v, triggers as tri, dependencies as dep where dep.id = v.id AND dep.depend_id =tri.id AND dep.depend_type = 8 AND v.type = 1;


--Function f1 has a dependency on function f2
SELECT f1.name, f2.name, 'DEP_FUNC' from functions as f1, functions as f2, dependencies as dep where f1.id = dep.id AND f2.id = dep.depend_id AND dep.depend_type = 7 order by f2.name, f1.name;

--Function f1 has a dependency on trigger tri
SELECT f.name, tri.name, 'DEP_TRIGGER' from functions as f, triggers as tri, dependencies as dep where dep.id = f.id AND dep.depend_id =tri.id AND dep.depend_type = 8;

--Key k has a dependency on foreign key fk
SELECT k.name, fk.name, 'DEP_FKEY' from keys as k, keys as fk where fk.rkey = k.id;



DROP TABLE t1;

DROP VIEW v1;

DROP TABLE t2; 

DROP FUNCTION f1;

DROP FUNCTION f2;

DROP TRIGGER trigger_test;

DROP INDEX id_index;

DROP INDEX id_age_index;


DROP TABLE t2; 

DROP FUNCTION f1;

DROP VIEW v1;

DROP VIEW v2;

DROP VIEW v1;

DROP TABLE t1;

DROP TRIGGER trigger_test_4;

DROP TABLE t1;

DROP TABLE t3;
