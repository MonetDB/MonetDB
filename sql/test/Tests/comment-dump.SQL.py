import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql',
                    stdin=open(os.path.join(os.getenv('TSTSRCDIR'),
                                              'comment-dump.sql')),
                    stdout=process.PIPE, stderr=process.PIPE) as c:
    out, err = c.communicate()
    if re.search(r'^[^#\n]', out, re.M):
        sys.stdout.write(out)
    if re.search(r'^[^#\n]', err, re.M):
        sys.stderr.write(err)

dump1 = '''\
START TRANSACTION;
CREATE SCHEMA "foo" AUTHORIZATION "monetdb";
COMMENT ON SCHEMA "foo" IS 'foo foo';
CREATE SEQUENCE "foo"."counter" AS INTEGER;
COMMENT ON SEQUENCE "foo"."counter" IS 'counting';
SET SCHEMA "foo";
CREATE TABLE "foo"."tab" (
 "i" INTEGER,
 "j" DECIMAL(4,2)
);
COMMENT ON TABLE "foo"."tab" IS 'table';
CREATE INDEX "idx" ON "foo"."tab" ("j", "i");
COMMENT ON INDEX "foo"."idx" IS 'index on j';
COMMENT ON COLUMN "foo"."tab"."i" IS 'ii';
COMMENT ON COLUMN "foo"."tab"."j" IS 'jj';
create view vivi as select * from tab;
COMMENT ON VIEW "foo"."vivi" IS 'phew';
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
ALTER SEQUENCE "foo"."counter" RESTART WITH 1 NO CYCLE;
SET SCHEMA "sys";
COMMIT;
'''

dump2 = '''\
START TRANSACTION;
SET SCHEMA "foo";
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
COMMIT;
'''

with process.client('sqldump',
                    stdout=process.PIPE, stderr=process.PIPE) as d:
    out, err = d.communicate()
    out = ' '.join(re.split('[ \t]+', out))
    if out != dump1:
        sys.stdout.write(out)
    if re.search(r'^[^#\n]', err, re.M):
        sys.stderr.write(err)

with process.client('sqldump', args=['-f'],
                    stdout=process.PIPE, stderr=process.PIPE) as d2:
    out, err = d2.communicate()
    out = ' '.join(re.split('[ \t]+', out))
    if out != dump2:
        sys.stdout.write(out)
    if re.search(r'^[^#\n]', err, re.M):
        sys.stderr.write(err)

with process.client('sql',
                    stdin=open(os.path.join(os.getenv('TSTSRCDIR'),
                                              'comment-dump-cleanup.sql')),
                    stdout=process.PIPE, stderr=process.PIPE) as p:
    out, err = p.communicate()
    if re.search(r'^[^#\n]', out, re.M):
        sys.stdout.write(out)
    if re.search(r'^[^#\n]', err, re.M):
        sys.stderr.write(err)
