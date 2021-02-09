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
            mdb.execute("CREATE TABLE t0(c0 CLOB, c1 boolean, c2 tinyint, c3 int, UNIQUE(c3, c1, c0, c2));").assertSucceeded()
            mdb.execute("INSERT INTO t0(c0, c1, c3, c2) VALUES ('a', false, 2, 6);").assertSucceeded().assertRowCount(1)
            mdb.execute("SELECT c0, c1, c2, c3 FROM t0;").assertSucceeded().assertDataResultMatch([('a', False, 6, 2)])
        s.communicate()

    with process.server(mapiport=port, dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=port, username="monetdb", password="monetdb")
            mdb.execute("SELECT c0, c1, c2, c3 FROM t0;").assertSucceeded().assertDataResultMatch([('a', False, 6, 2)])
            mdb.execute("DROP TABLE t0;").assertSucceeded()
        s.communicate()
