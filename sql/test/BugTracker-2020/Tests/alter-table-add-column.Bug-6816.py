import sys, os, pymonetdb

db = os.getenv("TSTDB")
port = int(os.getenv("MAPIPORT"))

client1 = pymonetdb.connect(database=db, port=port, autocommit=True)
cursor1 = client1.cursor()
cursor1.execute("""
CREATE TABLE "testVarcharToClob" ("varcharColumn" varchar(255));
INSERT INTO "testVarcharToClob" VALUES ('value1'), ('value2');
ALTER TABLE "testVarcharToClob" add "clobColumn" TEXT NULL;
UPDATE "testVarcharToClob" SET "clobColumn" = "varcharColumn";
ALTER TABLE "testVarcharToClob" drop "varcharColumn";
""")

client2 = pymonetdb.connect(database=db, port=port, autocommit=True)
cursor2 = client2.cursor()
cursor2.execute("""
INSERT INTO "testVarcharToClob" VALUES ('value3');
DROP TABLE "testVarcharToClob";
""")

cursor1.close()
client1.close()
cursor2.close()
client2.close()
