import os

from MonetDBtesting.sqltest import SQLTestCase

port = os.environ['MAPIPORT']
db = os.environ['TSTDB']

with SQLTestCase() as cli:
    cli.connect(username="monetdb", password="monetdb")
    cli.execute("""
    START TRANSACTION;
    CREATE TABLE "mct00" ("c0" TINYINT,"c1" BOOLEAN);
    INSERT INTO "mct00" VALUES (4, true), (NULL, false);
    create procedure "sys"."dict_compress"(sname string, tname string, cname string, ordered_values bool) external name "dict"."compress";
    COMMIT;

    CALL "sys"."dict_compress"('sys','mct00','c1',true);
    CREATE REMOTE TABLE "rmct00" ("c0" TINYINT,"c1" BOOLEAN) ON 'mapi:monetdb://localhost:%s/%s/sys/mct00';
    """ % (port, db)).assertSucceeded()

    cli.execute('SELECT mct00.c1 FROM mct00;') \
        .assertSucceeded().assertDataResultMatch([(True,),(False,)])
    cli.execute('SELECT rmct00.c1 FROM rmct00;') \
        .assertSucceeded().assertDataResultMatch([(True,),(False,)])

    cli.execute("""
    START TRANSACTION;
    DROP TABLE rmct00;
    DROP TABLE mct00;
    DROP PROCEDURE "sys"."dict_compress";
    COMMIT;""").assertSucceeded()
