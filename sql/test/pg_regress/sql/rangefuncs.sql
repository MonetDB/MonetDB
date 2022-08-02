SELECT name, setting FROM pg_settings WHERE name LIKE 'enable%';

CREATE TABLE foo2(fooid int, f2 int);
INSERT INTO foo2 VALUES(1, 11);
INSERT INTO foo2 VALUES(2, 22);
INSERT INTO foo2 VALUES(1, 111);

CREATE FUNCTION foot(int) returns setof foo2 as 'SELECT * FROM foo2 WHERE fooid = $1;' LANGUAGE SQL;

-- supposed to fail with ERROR
select * from foo2, foot(foo2.fooid) z where foo2.f2 = z.f2;

-- function in subselect
select * from foo2 where f2 in (select f2 from foot(foo2.fooid) z where z.fooid = foo2.fooid) ORDER BY 1,2;

-- function in subselect
select * from foo2 where f2 in (select f2 from foot(1) z where z.fooid = foo2.fooid) ORDER BY 1,2;

-- function in subselect
select * from foo2 where f2 in (select f2 from foot(foo2.fooid) z where z.fooid = 1) ORDER BY 1,2;

-- nested functions
select foot.fooid, foot.f2 from foot(sin(pi()/2)::int) ORDER BY 1,2;

CREATE TABLE foo (fooid int, foosubid int, fooname text, primary key(fooid,foosubid));
INSERT INTO foo VALUES(1,1,'Joe');
INSERT INTO foo VALUES(1,2,'Ed');
INSERT INTO foo VALUES(2,1,'Mary');

-- sql, proretset = f, prorettype = b
CREATE FUNCTION getfoo(int) RETURNS int AS 'SELECT $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = t, prorettype = b
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS setof int AS 'SELECT fooid FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = t, prorettype = b
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS setof text AS 'SELECT fooname FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = f, prorettype = c
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS foo AS 'SELECT * FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = t, prorettype = c
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS setof foo AS 'SELECT * FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- sql, proretset = f, prorettype = record
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS RECORD AS 'SELECT * FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1(fooid int, foosubid int, fooname text);
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1) AS 
(fooid int, foosubid int, fooname text);
SELECT * FROM vw_getfoo;

-- sql, proretset = t, prorettype = record
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS setof record AS 'SELECT * FROM foo WHERE fooid = $1;' LANGUAGE SQL;
SELECT * FROM getfoo(1) AS t1(fooid int, foosubid int, fooname text);
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1) AS
(fooid int, foosubid int, fooname text);
SELECT * FROM vw_getfoo;

-- plpgsql, proretset = f, prorettype = b
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS int AS 'DECLARE fooint int; BEGIN SELECT fooid into fooint FROM foo WHERE fooid = $1; RETURN fooint; END;' LANGUAGE 'plpgsql';
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

-- plpgsql, proretset = f, prorettype = c
DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
CREATE FUNCTION getfoo(int) RETURNS foo AS 'DECLARE footup foo%ROWTYPE; BEGIN SELECT * into footup FROM foo WHERE fooid = $1; RETURN footup; END;' LANGUAGE 'plpgsql';
SELECT * FROM getfoo(1) AS t1;
CREATE VIEW vw_getfoo AS SELECT * FROM getfoo(1);
SELECT * FROM vw_getfoo;

DROP VIEW vw_getfoo;
DROP FUNCTION getfoo(int);
DROP FUNCTION foot(int);
DROP TABLE foo2;
DROP TABLE foo;

-- Rescan tests --
CREATE TABLE foorescan (fooid int, foosubid int, fooname text, primary key(fooid,foosubid));
INSERT INTO foorescan values(5000,1,'abc.5000.1');
INSERT INTO foorescan values(5001,1,'abc.5001.1');
INSERT INTO foorescan values(5002,1,'abc.5002.1');
INSERT INTO foorescan values(5003,1,'abc.5003.1');
INSERT INTO foorescan values(5004,1,'abc.5004.1');
INSERT INTO foorescan values(5005,1,'abc.5005.1');
INSERT INTO foorescan values(5006,1,'abc.5006.1');
INSERT INTO foorescan values(5007,1,'abc.5007.1');
INSERT INTO foorescan values(5008,1,'abc.5008.1');
INSERT INTO foorescan values(5009,1,'abc.5009.1');

INSERT INTO foorescan values(5000,2,'abc.5000.2');
INSERT INTO foorescan values(5001,2,'abc.5001.2');
INSERT INTO foorescan values(5002,2,'abc.5002.2');
INSERT INTO foorescan values(5003,2,'abc.5003.2');
INSERT INTO foorescan values(5004,2,'abc.5004.2');
INSERT INTO foorescan values(5005,2,'abc.5005.2');
INSERT INTO foorescan values(5006,2,'abc.5006.2');
INSERT INTO foorescan values(5007,2,'abc.5007.2');
INSERT INTO foorescan values(5008,2,'abc.5008.2');
INSERT INTO foorescan values(5009,2,'abc.5009.2');

INSERT INTO foorescan values(5000,3,'abc.5000.3');
INSERT INTO foorescan values(5001,3,'abc.5001.3');
INSERT INTO foorescan values(5002,3,'abc.5002.3');
INSERT INTO foorescan values(5003,3,'abc.5003.3');
INSERT INTO foorescan values(5004,3,'abc.5004.3');
INSERT INTO foorescan values(5005,3,'abc.5005.3');
INSERT INTO foorescan values(5006,3,'abc.5006.3');
INSERT INTO foorescan values(5007,3,'abc.5007.3');
INSERT INTO foorescan values(5008,3,'abc.5008.3');
INSERT INTO foorescan values(5009,3,'abc.5009.3');

INSERT INTO foorescan values(5000,4,'abc.5000.4');
INSERT INTO foorescan values(5001,4,'abc.5001.4');
INSERT INTO foorescan values(5002,4,'abc.5002.4');
INSERT INTO foorescan values(5003,4,'abc.5003.4');
INSERT INTO foorescan values(5004,4,'abc.5004.4');
INSERT INTO foorescan values(5005,4,'abc.5005.4');
INSERT INTO foorescan values(5006,4,'abc.5006.4');
INSERT INTO foorescan values(5007,4,'abc.5007.4');
INSERT INTO foorescan values(5008,4,'abc.5008.4');
INSERT INTO foorescan values(5009,4,'abc.5009.4');

INSERT INTO foorescan values(5000,5,'abc.5000.5');
INSERT INTO foorescan values(5001,5,'abc.5001.5');
INSERT INTO foorescan values(5002,5,'abc.5002.5');
INSERT INTO foorescan values(5003,5,'abc.5003.5');
INSERT INTO foorescan values(5004,5,'abc.5004.5');
INSERT INTO foorescan values(5005,5,'abc.5005.5');
INSERT INTO foorescan values(5006,5,'abc.5006.5');
INSERT INTO foorescan values(5007,5,'abc.5007.5');
INSERT INTO foorescan values(5008,5,'abc.5008.5');
INSERT INTO foorescan values(5009,5,'abc.5009.5');

CREATE FUNCTION foorescan(int,int) RETURNS setof foorescan AS 'SELECT * FROM foorescan WHERE fooid >= $1 and fooid < $2 ;' LANGUAGE SQL;

--invokes ExecFunctionReScan
SELECT * FROM foorescan f WHERE f.fooid IN (SELECT fooid FROM foorescan(5002,5004)) ORDER BY 1,2;

CREATE VIEW vw_foorescan AS SELECT * FROM foorescan(5002,5004);

--invokes ExecFunctionReScan
SELECT * FROM foorescan f WHERE f.fooid IN (SELECT fooid FROM vw_foorescan) ORDER BY 1,2;

CREATE TABLE barrescan (fooid int primary key);
INSERT INTO barrescan values(5003);
INSERT INTO barrescan values(5004);
INSERT INTO barrescan values(5005);
INSERT INTO barrescan values(5006);
INSERT INTO barrescan values(5007);
INSERT INTO barrescan values(5008);

CREATE FUNCTION foorescan(int) RETURNS setof foorescan AS 'SELECT * FROM foorescan WHERE fooid = $1;' LANGUAGE SQL;

--invokes ExecFunctionReScan with chgParam != NULL
SELECT f.* FROM barrescan b, foorescan f WHERE f.fooid = b.fooid AND b.fooid IN (SELECT fooid FROM foorescan(b.fooid)) ORDER BY 1,2;
SELECT b.fooid, max(f.foosubid) FROM barrescan b, foorescan f WHERE f.fooid = b.fooid AND b.fooid IN (SELECT fooid FROM foorescan(b.fooid)) GROUP BY b.fooid ORDER BY 1,2;

CREATE VIEW fooview1 AS SELECT f.* FROM barrescan b, foorescan f WHERE f.fooid = b.fooid AND b.fooid IN (SELECT fooid FROM foorescan(b.fooid)) ORDER BY 1,2;
SELECT * FROM fooview1 AS fv WHERE fv.fooid = 5004;

CREATE VIEW fooview2 AS SELECT b.fooid, max(f.foosubid) AS maxsubid FROM barrescan b, foorescan f WHERE f.fooid = b.fooid AND b.fooid IN (SELECT fooid FROM foorescan(b.fooid)) GROUP BY b.fooid ORDER BY 1,2;
SELECT * FROM fooview2 AS fv WHERE fv.maxsubid = 5;

DROP VIEW vw_foorescan;
DROP VIEW fooview1;
DROP VIEW fooview2;
DROP FUNCTION foorescan(int,int);
DROP FUNCTION foorescan(int);
DROP TABLE foorescan;
DROP TABLE barrescan;
