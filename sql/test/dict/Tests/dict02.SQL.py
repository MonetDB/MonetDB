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

# if one transaction compresses a column, disallow concurrent inserts/updates/deletes on the table
with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")

        mdb1.execute("""
        START TRANSACTION;
        create table t0(c0 int);
        insert into t0 values (1),(2),(3);
        create procedure "sys"."dict_compress"(sname string, tname string, cname string, ordered_values bool) external name "dict"."compress";
        COMMIT;""").assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('call "sys"."dict_compress"(\'sys\',\'t0\',\'c0\',false);').assertSucceeded()
        mdb2.execute("insert into t0 values (4),(5),(6);").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40001", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")
        mdb1.execute('select c0 from t0;').assertSucceeded().assertDataResultMatch([(1,),(2,),(3,)])
        mdb2.execute('select c0 from t0;').assertSucceeded().assertDataResultMatch([(1,),(2,),(3,)])

        mdb1.execute("""
        START TRANSACTION;
        drop table t0;
        drop procedure "sys"."dict_compress";
        COMMIT;""").assertSucceeded()
