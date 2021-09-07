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
    os.mkdir(os.path.join(farm_dir, 'mydb'))

    with process.server(mapiport=port, dbname='mydb', dbfarm=os.path.join(farm_dir, 'mydb'),
                        stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(username="monetdb", password="monetdb")
            mdb.execute("create table test (col int);").assertSucceeded()
            mdb.execute("insert into test values (1), (2), (3);").assertSucceeded().assertRowCount(3)

            mdb.execute("begin transaction;").assertSucceeded()
            mdb.execute("create table test_new (col int);").assertSucceeded()
            mdb.execute("commit;").assertSucceeded()

            mdb.execute("begin transaction;").assertSucceeded()
            mdb.execute("truncate table test_new;").assertSucceeded()
            mdb.execute("insert into test_new select * from test;").assertSucceeded().assertRowCount(3)
            mdb.execute("drop table test;").assertSucceeded()
            mdb.execute("alter table test_new rename to test;").assertSucceeded()
            mdb.execute("commit;").assertSucceeded()

            mdb.execute("select * from test;").assertSucceeded().assertDataResultMatch([(1,),(2,),(3,)])
        s.communicate()

    with process.server(mapiport=port, dbname='mydb', dbfarm=os.path.join(farm_dir, 'mydb'),
                        stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(username="monetdb", password="monetdb")
            mdb.execute("select * from test;").assertSucceeded().assertDataResultMatch([(1,),(2,),(3,)])
            mdb.execute("drop table test;").assertSucceeded()
        s.communicate()
