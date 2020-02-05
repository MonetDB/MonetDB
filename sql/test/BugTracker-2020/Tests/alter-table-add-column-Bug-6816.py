import sys

try:
    from MonetDBtesting import process
except ImportError:
    import process

s = process.server(args = [], stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c1 = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)
c2 = process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

out, err = c1.communicate('''\
CREATE TABLE "testVarcharToClob" ("varcharColumn" varchar(255));\
INSERT INTO "testVarcharToClob" VALUES ('value1'), ('value2');\
ALTER TABLE "testVarcharToClob" add "clobColumn" TEXT NULL;\
UPDATE "testVarcharToClob" SET "clobColumn" = "varcharColumn";\
ALTER TABLE "testVarcharToClob" drop "varcharColumn";
''')
sys.stdout.write(out)
sys.stderr.write(err)

out, err = c2.communicate('''\
INSERT INTO "testVarcharToClob" VALUES ('value3');\
DROP TABLE "testVarcharToClob";
''')
sys.stdout.write(out)
sys.stderr.write(err)

out, err = s.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
