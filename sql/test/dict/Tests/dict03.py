import os, tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process
from MonetDBtesting.sqltest import SQLTestCase

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username="monetdb", password="monetdb")
            mdb.execute("""
            START TRANSACTION;
            create or replace procedure "sys"."dict_compress"(sname string, tname string, cname string) external name "dict"."compress";
            create or replace procedure "sys"."for_compress"(sname string, tname string, cname string) external name "for"."compress";
            CREATE TABLE "t1" ("c0" CLOB);
            INSERT INTO "t1" VALUES ('85'),('ieyE7bk'),('#2MP'),('v汉字'),('2');
            CREATE TABLE "t2" ("c0" BIGINT NOT NULL);
            INSERT INTO "t2" VALUES (-1981639662);
            COMMIT""").assertSucceeded()
            mdb.execute("SELECT c0 FROM t1 ORDER BY c0;").assertSucceeded().assertDataResultMatch([('#2MP',),('2',),('85',),('ieyE7bk',),('v汉字',)])
            mdb.execute("CALL \"sys\".\"dict_compress\"('sys','t1','c0');").assertSucceeded()
            mdb.execute("SELECT c0 FROM t1 ORDER BY c0;").assertSucceeded().assertDataResultMatch([('#2MP',),('2',),('85',),('ieyE7bk',),('v汉字',)])
            mdb.execute("TRUNCATE TABLE t1;").assertSucceeded().assertRowCount(5)
            mdb.execute("SELECT c0 FROM t1 ORDER BY c0;").assertSucceeded().assertDataResultMatch([])
            mdb.execute("INSERT INTO t1(c0) VALUES(''), ('3be汉字0'), ('aa8877');").assertSucceeded().assertRowCount(3)
            mdb.execute("SELECT c0 FROM t1 ORDER BY c0;").assertSucceeded().assertDataResultMatch([('',),('3be汉字0',),('aa8877',)])
            mdb.execute("SELECT c0 FROM t2;").assertSucceeded().assertDataResultMatch([(-1981639662,),])
            mdb.execute("CALL \"sys\".\"for_compress\"('sys','t2','c0');").assertSucceeded()
            mdb.execute("SELECT c0 FROM t2;").assertSucceeded().assertDataResultMatch([(-1981639662,),])
        s.communicate()

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username="monetdb", password="monetdb")
            mdb.execute("SELECT c0 FROM t1 ORDER BY c0;").assertSucceeded().assertDataResultMatch([('',),('3be汉字0',),('aa8877',)])
            mdb.execute("SELECT c0 FROM t2;").assertSucceeded().assertDataResultMatch([(-1981639662,),])
        s.communicate()

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username="monetdb", password="monetdb")
            mdb.execute("SELECT c0 FROM t1 ORDER BY c0;").assertSucceeded().assertDataResultMatch([('',),('3be汉字0',),('aa8877',)])
            mdb.execute("SELECT c0 FROM t2;").assertSucceeded().assertDataResultMatch([(-1981639662,),])
            mdb.execute("""
            START TRANSACTION;
            DROP TABLE t1;
            DROP TABLE t2;
            DROP ALL PROCEDURE "sys"."dict_compress";
            DROP ALL PROCEDURE "sys"."for_compress";
            COMMIT;
            """).assertSucceeded()
        s.communicate()
