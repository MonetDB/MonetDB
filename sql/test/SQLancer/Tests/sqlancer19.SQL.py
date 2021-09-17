import os

from MonetDBtesting.sqltest import SQLTestCase

port = os.environ['MAPIPORT']
db = os.environ['TSTDB']

with SQLTestCase() as cli:
    cli.connect(username="monetdb", password="monetdb")
    cli.execute("""
    START TRANSACTION;
    CREATE TABLE "t3" ("c0" BIGINT,"c1" INTERVAL MONTH);
    INSERT INTO "t3" VALUES (1, INTERVAL '9' MONTH),(5, INTERVAL '6' MONTH),(5, NULL),(7, NULL),(2, INTERVAL '1' MONTH),(2, INTERVAL '1' MONTH);
    COMMIT;
    START TRANSACTION;
    CREATE REMOTE TABLE "rt3" ("c0" BIGINT,"c1" INTERVAL MONTH) ON 'mapi:monetdb://localhost:%s/%s/sys/t3';
    COMMIT;""" % (port, db)).assertSucceeded()

    cli.execute('SELECT json."integer"(JSON \'1\') FROM rt3;').assertSucceeded().assertDataResultMatch([(1,),(1,),(1,),(1,),(1,),(1,)])

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rt3;
    DROP TABLE t3;
    COMMIT;""").assertSucceeded()
