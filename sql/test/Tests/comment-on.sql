CREATE SCHEMA foo;
SET SCHEMA foo;

/* initially, no comments visible */
\dn
\dn foo

/* comment can be set */
COMMENT ON SCHEMA foo IS 'foo foo';
\dn
\dn foo

/* comment can be changed */
COMMENT ON SCHEMA foo IS 'bar bar';
\dn
\dn foo

/* comment can be deleted by setting to null */
COMMENT ON SCHEMA foo IS NULL;
\dn
\dn foo

/* comment can be restored */
COMMENT ON SCHEMA foo IS 'foo bar';
\dn
\dn foo

/* comment can also be deleted by setting to '' */
COMMENT ON SCHEMA foo IS '';
\dn
\dn foo

/* leave the comment set so we can test msqldump */
COMMENT ON SCHEMA foo IS 'one final comment';

/* finally, awkward names work as well */
CREATE SCHEMA "space separated";
COMMENT ON SCHEMA "space separated" IS 'space separated';
\dn
\dn "space separated"
DROP SCHEMA "space separated";

/* TABLES */

\d

/* initially, no comment */
CREATE TABLE tab(i INT, j DECIMAL(4,2));
\d

/* comments show both in the table list and the description */
COMMENT ON TABLE tab IS 'table';
\d
\d tab

/* qualified tables also work */
COMMENT ON TABLE foo.tab IS 'qualified table';
\d
\d tab

/* even when referring to another schema */
SET SCHEMA sys;
COMMENT ON TABLE foo.tab IS 'table';
SET SCHEMA foo;
\d
\d tab

/* views show both in the table list and in the description */
CREATE VIEW vivi AS SELECT * FROM tab;
COMMENT ON VIEW vivi IS 'phew';
\dv
\d vivi

/* comment on table does not work on views and vice versa */
COMMENT ON VIEW tab IS '';
COMMENT ON TABLE vivi IS '';
\d

/* cannot comment on temporary table */
CREATE TEMPORARY TABLE tempo(LIKE foo.tab);
COMMENT ON TABLE tempo IS 'temporary';
COMMENT ON TABLE tmp.tempo IS 'temporary';

/* commenting on columns works both with and without schema. */
/* also, column comments are listed in table order, not addition order */
COMMENT ON COLUMN tab.j IS 'jj';
COMMENT ON COLUMN foo.tab.i IS 'ii';
\d tab

COMMENT ON COLUMN vivi.j IS 'vjj';
COMMENT ON COLUMN foo.vivi.i IS 'vii';
\d vivi

/* comment on index works */
CREATE INDEX idx ON tab(j,i);
COMMENT ON INDEX idx IS 'index on j';
\d tab

/* comment on sequence works */
CREATE SEQUENCE counter AS INT;
\ds
COMMENT ON SEQUENCE counter IS 'counting';
\ds
\ds counter

/* comment on schema.sequence also works */
SET SCHEMA sys;
COMMENT ON SEQUENCE foo.counter IS 'still counting';
SET SCHEMA foo;
\ds

/* comment on function works */
CREATE FUNCTION f() RETURNS INT BEGIN RETURN 42; END;
COMMENT ON FUNCTION f() IS '0 parms';

CREATE FUNCTION f(i INT) RETURNS INT BEGIN RETURN 43; END;
COMMENT ON FUNCTION f(INT) IS '1 parm';

CREATE FUNCTION f(i INT, j INT) RETURNS INT BEGIN RETURN 44; END;
COMMENT ON FUNCTION f(INTEGER, INTEGER) IS '2 parms';

/* these two are merged in \df but shown separately in \df f */
CREATE FUNCTION f(i INT, j INT, k INT) RETURNS INT BEGIN RETURN 45; END;
CREATE FUNCTION f(i INT, j INT, k INT, l INT) RETURNS INT BEGIN RETURN 45; END;

CREATE PROCEDURE g() BEGIN DELETE FROM tab WHERE FALSE; END;
COMMENT ON PROCEDURE g() IS 'proc';

\df
\df f
\df g
\df foo.g

-- if there is no ambiguity we can leave out the parameter list
COMMENT ON PROCEDURE g IS 'proc!';
\df g

-- if there is ambiguity we can't
COMMENT ON FUNCTION f IS 'ambiguous';


-- test all COMMENT ON <db-objecttype> variations with an object name which does not exist, so should report a "no such <db-objecttype>: abc" error
COMMENT ON SCHEMA "abc" IS 'schema abc';
COMMENT ON TABLE "abc" IS 'table abc';
COMMENT ON VIEW "abc" IS 'view abc';
COMMENT ON COLUMN "abc".abc IS 'column abc';
COMMENT ON INDEX "abc" IS 'index abc';
COMMENT ON SEQUENCE "abc" IS 'seq abc';
COMMENT ON FUNCTION "abc" IS 'function abc';
COMMENT ON PROCEDURE "abc" IS 'procedure abc';
COMMENT ON AGGREGATE "abc" IS 'aggregate abc';
COMMENT ON FILTER FUNCTION "abc" IS 'filter function abc';
COMMENT ON LOADER "abc" IS 'loader abc';

-- test COMMENT ON <db-objecttype> for db-objecttypes for which we do NOT support comments to be set, so should report a syntax error
COMMENT ON TYPE "int" IS 'signed integer number 32 bits';
COMMENT ON TRIGGER "abc" IS 'trigger abc';
COMMENT ON PRIMARY KEY "abc" IS 'primary key abc';
COMMENT ON FOREIGN KEY "abc" IS 'foreign key abc';
COMMENT ON UNIQUE KEY "abc" IS 'unique key abc';
COMMENT ON KEY "abc" IS 'key abc';
COMMENT ON FUNCTION ARGUMENT abc.i IS 'function argument abc.i';
COMMENT ON PROCEDURE ARGUMENT abc.i IS 'procedure argument abc.i';
COMMENT ON DATABASE "abc" IS 'database abc';


-- before cleanup show the created comments
SELECT remark FROM sys.comments order by remark;

-- cleanup
DROP FUNCTION f();
DROP FUNCTION f(INT);
DROP FUNCTION f(INT,INT);
DROP FUNCTION f(INT,INT,INT);
DROP FUNCTION f(INT,INT,INT,INT);
DROP PROCEDURE g();

DROP SEQUENCE counter;
DROP VIEW vivi;
DROP TABLE tab;

SET SCHEMA sys;
DROP SCHEMA foo;

-- after dropping all the created objects, the comments should be removed also
SELECT remark FROM sys.comments order by remark;

