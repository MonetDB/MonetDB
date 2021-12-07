import os, socket, tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process
from MonetDBtesting.sqltest import SQLTestCase

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(('', 0))
port = sock.getsockname()[1]
sock.close()

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with process.server(mapiport=port, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=port, username="monetdb", password="monetdb")
            mdb.execute("""
            START TRANSACTION;
            create procedure "sys"."dict_compress"(sname string, tname string, cname string) external name "dict"."compress";
            CREATE TABLE "t1" ("c0" CLOB);
            INSERT INTO "t1" VALUES ('85'),('ieyE7bk'),('#2MP'),('v汉字'),('2');
            COMMIT""").assertSucceeded()
            mdb.execute("SELECT c0 FROM t1").assertSucceeded().assertDataResultMatch([('85',),('ieyE7bk',),('#2MP',),('v汉字',),('2',)])
            mdb.execute("CALL \"sys\".\"dict_compress\"('sys','t1','c0');").assertSucceeded()
            mdb.execute("SELECT c0 FROM t1").assertSucceeded().assertDataResultMatch([('85',),('ieyE7bk',),('#2MP',),('v汉字',),('2',)])
            mdb.execute("TRUNCATE TABLE t1;").assertSucceeded().assertRowCount(5)
            mdb.execute("SELECT c0 FROM t1").assertSucceeded().assertDataResultMatch([])
            mdb.execute("INSERT INTO t1(c0) VALUES(''), ('3be汉字0'), ('aa8877');").assertSucceeded().assertRowCount(3)
            mdb.execute("SELECT c0 FROM t1").assertSucceeded().assertDataResultMatch([('',),('3be汉字0',),('aa8877',)])
        s.communicate()

    with process.server(mapiport=port, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=port, username="monetdb", password="monetdb")
            mdb.execute("SELECT c0 FROM t1").assertSucceeded().assertDataResultMatch([('',),('3be汉字0',),('aa8877',)])
            mdb.execute("""
            START TRANSACTION;
            DROP TABLE t1;
            DROP ALL PROCEDURE "sys"."dict_compress";
            COMMIT;
            """).assertSucceeded()
        s.communicate()
