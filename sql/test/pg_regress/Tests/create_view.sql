--
-- CREATE_VIEW
-- Virtual class definitions
--	(this also tests the query rewrite system)
--

CREATE VIEW street AS
   SELECT r.name, r.thepath, c.cname AS cname
    FROM road r, real_city c
   WHERE c.outline = r.thepath;

CREATE VIEW iexit AS
   SELECT ih.name, ih.thepath /* ,
   funtion interpt_pp() does not exist, so exclude it
	interpt_pp(ih.thepath, r.thepath) AS exit */
   FROM ihighway ih, ramp r
   WHERE ih.thepath = r.thepath;

CREATE VIEW toyemp AS
   SELECT name, age, location, 12*salary AS annualsal
   FROM emp;

-- Test comments




--
-- CREATE OR REPLACE VIEW
-- MonetDB does not (yet) support CREATE OR REPLACE VIEW,  see https://www.monetdb.org/bugzilla/show_bug.cgi?id=3574
--

CREATE TABLE viewtest_tbl (a int, b int);
/*
COPY INTO viewtest_tbl FROM stdin;
5	10
10	15
15	20
20	25
\.
*/
INSERT INTO viewtest_tbl VALUES (5, 10), (10, 15), (15, 20), (20, 25);

CREATE /* OR REPLACE */ VIEW viewtest AS
	SELECT * FROM viewtest_tbl;

CREATE OR REPLACE VIEW viewtest AS
	SELECT * FROM viewtest_tbl WHERE a > 10;
-- MonetDB does not (yet) support CREATE OR REPLACE. test with DROP VIEW and CREATE VIEW instead
DROP VIEW viewtest;
CREATE VIEW viewtest AS
	SELECT * FROM viewtest_tbl WHERE a > 10;

SELECT * FROM viewtest;

CREATE OR REPLACE VIEW viewtest AS
	SELECT a, b FROM viewtest_tbl WHERE a > 5 ORDER BY b DESC;

DROP VIEW viewtest;
CREATE VIEW viewtest AS
	SELECT a, b FROM viewtest_tbl WHERE a > 5 ORDER BY b DESC;

SELECT * FROM viewtest;

-- should fail
CREATE OR REPLACE VIEW viewtest AS
	SELECT a FROM viewtest_tbl WHERE a <> 20;
DROP VIEW viewtest;
CREATE VIEW viewtest AS
	SELECT a FROM viewtest_tbl WHERE a <> 20;

-- should fail
CREATE OR REPLACE VIEW viewtest AS
	SELECT 1, * FROM viewtest_tbl;
DROP VIEW viewtest;
CREATE VIEW viewtest AS
	SELECT 1, * FROM viewtest_tbl;

-- should fail
CREATE OR REPLACE VIEW viewtest AS
	SELECT a, cast(b as numeric) FROM viewtest_tbl;
DROP VIEW viewtest;
CREATE VIEW viewtest AS
	SELECT a, cast(b as numeric) FROM viewtest_tbl;

DROP VIEW viewtest;
DROP TABLE viewtest_tbl;

