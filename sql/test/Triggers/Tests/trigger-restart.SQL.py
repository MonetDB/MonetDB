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
            mdb.execute("CREATE TABLE x(x INT);").assertSucceeded()
            mdb.execute("CREATE TABLE y(y INT);").assertSucceeded()
            mdb.execute("CREATE TRIGGER myt after insert on x referencing new row as n for each statement insert into y values(n.x);").assertSucceeded()
            mdb.execute("INSERT INTO x VALUES (1);").assertSucceeded()
            mdb.execute("SELECT x FROM x ORDER BY x;").assertSucceeded().assertDataResultMatch([(1,)])
            mdb.execute("SELECT y FROM y ORDER BY y;").assertSucceeded().assertDataResultMatch([(1,)])
        s.communicate()

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username="monetdb", password="monetdb")
            mdb.execute("INSERT INTO x VALUES (2);").assertSucceeded()
            mdb.execute("SELECT x FROM x ORDER BY x;").assertSucceeded().assertDataResultMatch([(1,),(2,)])
            mdb.execute("SELECT y FROM y ORDER BY y;").assertSucceeded().assertDataResultMatch([(1,),(2,)])
            mdb.execute("DROP TRIGGER myt;").assertSucceeded()
            mdb.execute("DROP TABLE x;").assertSucceeded()
            mdb.execute("DROP TABLE y;").assertSucceeded()
        s.communicate()
