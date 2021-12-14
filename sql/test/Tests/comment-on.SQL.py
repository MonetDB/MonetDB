import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

dump = '''\
#CREATE SCHEMA foo;
#SET SCHEMA foo;
SCHEMA foo
CREATE SCHEMA "foo" AUTHORIZATION "monetdb";
#COMMENT ON SCHEMA foo IS 'foo foo';
SCHEMA foo 'foo foo'
CREATE SCHEMA "foo" AUTHORIZATION "monetdb";
COMMENT ON SCHEMA "foo" IS 'foo foo';
#COMMENT ON SCHEMA foo IS 'bar bar';
SCHEMA foo 'bar bar'
CREATE SCHEMA "foo" AUTHORIZATION "monetdb";
COMMENT ON SCHEMA "foo" IS 'bar bar';
#COMMENT ON SCHEMA foo IS NULL;
SCHEMA foo
CREATE SCHEMA "foo" AUTHORIZATION "monetdb";
#COMMENT ON SCHEMA foo IS 'foo bar';
SCHEMA foo 'foo bar'
CREATE SCHEMA "foo" AUTHORIZATION "monetdb";
COMMENT ON SCHEMA "foo" IS 'foo bar';
#COMMENT ON SCHEMA foo IS '';
SCHEMA foo
CREATE SCHEMA "foo" AUTHORIZATION "monetdb";
#COMMENT ON SCHEMA foo IS 'one final comment';
#CREATE SCHEMA "space separated";
#COMMENT ON SCHEMA "space separated" IS 'space separated';
SCHEMA foo 'one final comment'
SCHEMA space separated 'space separated'
CREATE SCHEMA "space separated" AUTHORIZATION "monetdb";
COMMENT ON SCHEMA "space separated" IS 'space separated';
#DROP SCHEMA "space separated";
#CREATE TABLE tab(i INT, j DECIMAL(4,2));
TABLE foo.tab
#COMMENT ON TABLE tab IS 'table';
TABLE foo.tab 'table'
CREATE TABLE "foo"."tab" (
 "i" INTEGER,
 "j" DECIMAL(4,2)
);
COMMENT ON TABLE "foo"."tab" IS 'table';
#COMMENT ON TABLE foo.tab IS 'qualified table';
TABLE foo.tab 'qualified table'
CREATE TABLE "foo"."tab" (
 "i" INTEGER,
 "j" DECIMAL(4,2)
);
COMMENT ON TABLE "foo"."tab" IS 'qualified table';
#SET SCHEMA sys;
#COMMENT ON TABLE foo.tab IS 'table';
#SET SCHEMA foo;
TABLE foo.tab 'table'
CREATE TABLE "foo"."tab" (
 "i" INTEGER,
 "j" DECIMAL(4,2)
);
COMMENT ON TABLE "foo"."tab" IS 'table';
#CREATE VIEW vivi AS SELECT * FROM tab;
#COMMENT ON VIEW vivi IS 'phew';
VIEW foo.vivi 'phew'
create view vivi as select * from tab;
COMMENT ON VIEW "foo"."vivi" IS 'phew';
TABLE foo.tab 'table'
VIEW foo.vivi 'phew'
#CREATE TEMPORARY TABLE tempo(LIKE foo.tab);
#COMMENT ON COLUMN tab.j IS 'jj';
#COMMENT ON COLUMN foo.tab.i IS 'ii';
CREATE TABLE "foo"."tab" (
 "i" INTEGER,
 "j" DECIMAL(4,2)
);
COMMENT ON TABLE "foo"."tab" IS 'table';
COMMENT ON COLUMN "foo"."tab"."i" IS 'ii';
COMMENT ON COLUMN "foo"."tab"."j" IS 'jj';
#COMMENT ON COLUMN vivi.j IS 'vjj';
#COMMENT ON COLUMN foo.vivi.i IS 'vii';
create view vivi as select * from tab;
COMMENT ON VIEW "foo"."vivi" IS 'phew';
COMMENT ON COLUMN "foo"."vivi"."i" IS 'vii';
COMMENT ON COLUMN "foo"."vivi"."j" IS 'vjj';
#CREATE INDEX idx ON tab(j,i);
#COMMENT ON INDEX idx IS 'index on j';
CREATE TABLE "foo"."tab" (
 "i" INTEGER,
 "j" DECIMAL(4,2)
);
COMMENT ON TABLE "foo"."tab" IS 'table';
CREATE INDEX "idx" ON "foo"."tab" ("j", "i");
COMMENT ON INDEX "foo"."idx" IS 'index on j';
COMMENT ON COLUMN "foo"."tab"."i" IS 'ii';
COMMENT ON COLUMN "foo"."tab"."j" IS 'jj';
#CREATE SEQUENCE counter AS INT;
SEQUENCE foo.counter
#COMMENT ON SEQUENCE counter IS 'counting';
SEQUENCE foo.counter 'counting'
CREATE SEQUENCE "foo"."counter" START WITH 1 NO CYCLE;
COMMENT ON SEQUENCE "foo"."counter" IS 'counting';
#SET SCHEMA sys;
#COMMENT ON SEQUENCE foo.counter IS 'still counting';
#SET SCHEMA foo;
SEQUENCE foo.counter 'still counting'
#CREATE FUNCTION f() RETURNS INT BEGIN RETURN 42; END;
#COMMENT ON FUNCTION f() IS '0 parms';
#CREATE FUNCTION f(i INT) RETURNS INT BEGIN RETURN 43; END;
#COMMENT ON FUNCTION f(INT) IS '1 parm';
#CREATE FUNCTION f(i INT, j INT) RETURNS INT BEGIN RETURN 44; END;
#COMMENT ON FUNCTION f(INTEGER, INTEGER) IS '2 parms';
#CREATE FUNCTION f(i INT, j INT, k INT) RETURNS INT BEGIN RETURN 45; END;
#CREATE FUNCTION f(i INT, j INT, k INT, l INT) RETURNS INT BEGIN RETURN 45; END;
#CREATE PROCEDURE g() BEGIN DELETE FROM tab WHERE FALSE; END;
#COMMENT ON PROCEDURE g() IS 'proc';
FUNCTION foo.f
FUNCTION foo.f '0 parms'
FUNCTION foo.f '1 parm'
FUNCTION foo.f '2 parms'
PROCEDURE foo.g 'proc'
create function f() returns int begin return 42; end;
COMMENT ON FUNCTION "foo"."f"() IS '0 parms';
create function f(i int) returns int begin return 43; end;
COMMENT ON FUNCTION "foo"."f"(INTEGER) IS '1 parm';
create function f(i int, j int) returns int begin return 44; end;
COMMENT ON FUNCTION "foo"."f"(INTEGER, INTEGER) IS '2 parms';
create function f(i int, j int, k int) returns int begin return 45; end;
create function f(i int, j int, k int, l int) returns int begin return 45; end;
create procedure g() begin delete from tab where false; end;
COMMENT ON PROCEDURE "foo"."g"() IS 'proc';
create procedure g() begin delete from tab where false; end;
COMMENT ON PROCEDURE "foo"."g"() IS 'proc';
#COMMENT ON PROCEDURE g IS 'proc!';
create procedure g() begin delete from tab where false; end;
COMMENT ON PROCEDURE "foo"."g"() IS 'proc!';
#SELECT remark FROM sys.comments order by remark;
% sys.comments # table_name
% remark # name
% varchar # type
% 17 # length
[ "0 parms" ]
[ "1 parm" ]
[ "2 parms" ]
[ "ii" ]
[ "index on j" ]
[ "jj" ]
[ "one final comment" ]
[ "phew" ]
[ "proc!" ]
[ "still counting" ]
[ "table" ]
[ "vii" ]
[ "vjj" ]
#DROP FUNCTION f();
#DROP FUNCTION f(INT);
#DROP FUNCTION f(INT,INT);
#DROP FUNCTION f(INT,INT,INT);
#DROP FUNCTION f(INT,INT,INT,INT);
#DROP PROCEDURE g();
#DROP SEQUENCE counter;
#DROP VIEW vivi;
#DROP TABLE tab;
#SET SCHEMA sys;
#DROP SCHEMA foo;
#SELECT remark FROM sys.comments order by remark;
% sys.comments # table_name
% remark # name
% varchar # type
% 0 # length
'''

edump = '''\
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON VIEW tab IS '';
ERROR = !COMMENT ON: no such view: foo.tab
CODE = 42S02
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON TABLE vivi IS '';
ERROR = !COMMENT ON: no such table: foo.vivi
CODE = 42S02
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON TABLE tempo IS 'temporary';
ERROR = !COMMENT ON tmp object not allowed
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON TABLE tmp.tempo IS 'temporary';
ERROR = !COMMENT ON tmp object not allowed
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON FUNCTION f IS 'ambiguous';
ERROR = !COMMENT FUNCTION: there are more than one function called 'f', please use the full signature
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON SCHEMA "abc" IS 'schema abc';
ERROR = !COMMENT ON: no such schema: abc
CODE = 3F000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON TABLE "abc" IS 'table abc';
ERROR = !COMMENT ON: no such table: foo.abc
CODE = 42S02
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON VIEW "abc" IS 'view abc';
ERROR = !COMMENT ON: no such view: foo.abc
CODE = 42S02
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON COLUMN "abc".abc IS 'column abc';
ERROR = !COMMENT ON: no such table: foo.abc
CODE = 42S02
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON INDEX "abc" IS 'index abc';
ERROR = !COMMENT ON: no such index: foo.abc
CODE = 42S12
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON SEQUENCE "abc" IS 'seq abc';
ERROR = !COMMENT ON: no such sequence: foo.abc
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON FUNCTION "abc" IS 'function abc';
ERROR = !COMMENT FUNCTION: no such function 'abc'
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON PROCEDURE "abc" IS 'procedure abc';
ERROR = !COMMENT PROCEDURE: no such procedure 'abc'
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON AGGREGATE "abc" IS 'aggregate abc';
ERROR = !COMMENT AGGREGATE: no such aggregate 'abc'
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON FILTER FUNCTION "abc" IS 'filter function abc';
ERROR = !COMMENT FILTER FUNCTION: no such filter function 'abc'
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON LOADER "abc" IS 'loader abc';
ERROR = !COMMENT LOADER FUNCTION: no such loader function 'abc'
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON TYPE "int" IS 'signed integer number 32 bits';
ERROR = !syntax error, unexpected TYPE in: "comment on type"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON TRIGGER "abc" IS 'trigger abc';
ERROR = !syntax error, unexpected TRIGGER in: "comment on trigger"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON PRIMARY KEY "abc" IS 'primary key abc';
ERROR = !syntax error, unexpected PRIMARY in: "comment on primary"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON FOREIGN KEY "abc" IS 'foreign key abc';
ERROR = !syntax error, unexpected FOREIGN in: "comment on foreign"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON UNIQUE KEY "abc" IS 'unique key abc';
ERROR = !syntax error, unexpected UNIQUE in: "comment on unique"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON KEY "abc" IS 'key abc';
ERROR = !syntax error, unexpected KEY in: "comment on key"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON FUNCTION ARGUMENT abc.i IS 'function argument abc.i';
ERROR = !syntax error, unexpected IDENT, expecting IS in: "comment on function argument abc"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON PROCEDURE ARGUMENT abc.i IS 'procedure argument abc.i';
ERROR = !syntax error, unexpected IDENT, expecting IS in: "comment on procedure argument abc"
CODE = 42000
MAPI = (monetdb) /var/tmp/mtest-\d+/.s.monetdb.\d+
QUERY = COMMENT ON DATABASE "abc" IS 'database abc';
ERROR = !syntax error, unexpected IDENT in: "comment on database"
CODE = 42000
'''

with process.client('sql',
                    stdin=open(os.path.join(os.getenv('TSTSRCDIR'),
                                              'comment-on.sql')),
                    interactive=True, echo=True,
                    stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate()
    out = ' '.join(re.split('[ \t]+', out))
    if out != dump:
        sys.stdout.write(out)
    if re.match(edump, err) is not None:
        sys.stderr.write(err)
