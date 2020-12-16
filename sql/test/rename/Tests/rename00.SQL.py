import os, socket, tempfile

from MonetDBtesting.sqltest import SQLTestCase

try:
    from MonetDBtesting import process
except ImportError:
    import process

def freeport():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))
    myport = freeport()

    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin = process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute('create table "something" (a int);').assertSucceeded()
            tc.execute('alter table "something" rename to "newname";').assertSucceeded()
            tc.execute('insert into "newname" values (1);').assertSucceeded().assertRowCount(1)
            tc.execute('select "a" from "newname";').assertSucceeded().assertDataResultMatch([(1,)])
        s.communicate()
    with process.server(mapiport=myport, dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=myport, database='db1')
            tc.execute('select "name" from sys.tables where "name" = \'newname\';').assertSucceeded()
            tc.execute('insert into "newname" values (2);').assertSucceeded().assertRowCount(1)
            tc.execute('select "a" from "newname";').assertSucceeded().assertDataResultMatch([(1,),(2,)])
            tc.execute('drop table "newname";').assertSucceeded()
        s.communicate()
