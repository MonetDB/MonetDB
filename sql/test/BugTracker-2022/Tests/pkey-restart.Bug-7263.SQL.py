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
            mdb.execute("CREATE TABLE x (k int PRIMARY KEY, v int);").assertSucceeded()
            mdb.execute("INSERT INTO x VALUES(1, 1);").assertSucceeded()
            mdb.execute("SELECT k, v FROM x;").assertSucceeded().assertDataResultMatch([(1,1)])
            mdb.execute("INSERT INTO x VALUES(1, 2);").assertFailed(err_code="40002", err_message="INSERT INTO: PRIMARY KEY constraint 'x.x_k_pkey' violated")
            mdb.execute("SELECT k, v FROM x;").assertSucceeded().assertDataResultMatch([(1,1)])
        s.communicate()

    with process.server(mapiport='0', dbname='db1', dbfarm=os.path.join(farm_dir, 'db1'), stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE) as s:
        with SQLTestCase() as mdb:
            mdb.connect(database='db1', port=s.dbport, username="monetdb", password="monetdb")
            mdb.execute("SELECT k, v FROM x;").assertSucceeded().assertDataResultMatch([(1,1)])
            mdb.execute("INSERT INTO x VALUES(1, 2);").assertFailed(err_code="40002", err_message="INSERT INTO: PRIMARY KEY constraint 'x.x_k_pkey' violated")
            mdb.execute("SELECT k, v FROM x;").assertSucceeded().assertDataResultMatch([(1,1)])
            mdb.execute("DROP TABLE x;").assertSucceeded()
        s.communicate()
