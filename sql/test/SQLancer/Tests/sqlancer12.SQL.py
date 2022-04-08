import os, tempfile

try:
    from MonetDBtesting import process
except ImportError:
    import process
from MonetDBtesting.sqltest import SQLTestCase

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username="monetdb", password="monetdb")
            mdb.execute("CREATE TABLE t0(c0 CLOB, c1 boolean, c2 tinyint, c3 int, UNIQUE(c3, c1, c0, c2));").assertSucceeded()
            mdb.execute("INSERT INTO t0(c0, c1, c3, c2) VALUES ('a', false, 2, 6);").assertSucceeded().assertRowCount(1)
            mdb.execute("SELECT c0, c1, c2, c3 FROM t0;").assertSucceeded().assertDataResultMatch([('a', False, 6, 2)])
            mdb.execute('CREATE TABLE "t1" ("c0" INT,"c2" INT,CONSTRAINT "con3" UNIQUE ("c0"));').assertSucceeded()
            mdb.execute("ALTER TABLE t1 DROP c0 CASCADE;").assertSucceeded()
        s.communicate()

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username="monetdb", password="monetdb")
            mdb.execute("SELECT c0, c1, c2, c3 FROM t0;").assertSucceeded().assertDataResultMatch([('a', False, 6, 2)])
            mdb.execute("DROP TABLE t0;").assertSucceeded()
            mdb.execute("DROP TABLE t1;").assertSucceeded()
        s.communicate()
