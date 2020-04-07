import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

with process.client('sql', stdin=process.PIPE, stdout=process.PIPE,
                    stderr=process.PIPE) as c1, \
     process.client('sql', stdin=process.PIPE, stdout=process.PIPE,
                    stderr=process.PIPE) as c2:
    out, err = c1.communicate('''\
CREATE TABLE "testVarcharToClob" ("varcharColumn" varchar(255));
INSERT INTO "testVarcharToClob" VALUES ('value1'), ('value2');
ALTER TABLE "testVarcharToClob" add "clobColumn" TEXT NULL;
UPDATE "testVarcharToClob" SET "clobColumn" = "varcharColumn";
ALTER TABLE "testVarcharToClob" drop "varcharColumn";
''')
    sys.stdout.write(out)
    sys.stderr.write(err)

    out, err = c2.communicate('''\
INSERT INTO "testVarcharToClob" VALUES ('value3');
DROP TABLE "testVarcharToClob";
''')
    sys.stdout.write(out)
    sys.stderr.write(err)
