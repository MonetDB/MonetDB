import os, tempfile

from MonetDBtesting.sqltest import SQLTestCase

try:
    from MonetDBtesting import process
except ImportError:
    import process

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin = process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=s.dbport, database='db1')
            tc.execute('create table "something" (a int);').assertSucceeded()
            tc.execute('alter table "something" rename to "newname";').assertSucceeded()
            tc.execute('insert into "newname" values (1);').assertSucceeded().assertRowCount(1)
            tc.execute('select "a" from "newname";').assertSucceeded().assertDataResultMatch([(1,)])
        s.communicate()
    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=s.dbport, database='db1')
            tc.execute('select "name" from sys.tables where "name" = \'newname\';').assertSucceeded()
            tc.execute('insert into "newname" values (2);').assertSucceeded().assertRowCount(1)
            tc.execute('select "a" from "newname";').assertSucceeded().assertDataResultMatch([(1,),(2,)])
            tc.execute('drop table "newname";').assertSucceeded()
        s.communicate()
