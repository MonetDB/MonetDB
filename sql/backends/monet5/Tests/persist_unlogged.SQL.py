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
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=s.dbport, database='db1')
            tc.execute("CREATE SCHEMA put").assertSucceeded()
            tc.execute("SET SCHEMA put").assertSucceeded()
            tc.execute("CREATE UNLOGGED TABLE foo (x INT)").assertSucceeded()
            tc.execute("ALTER TABLE foo SET INSERT ONLY").assertSucceeded()
            tc.execute("INSERT INTO foo SELECT * FROM generate_series(0,500)")
            tc.execute("SELECT count(*) FROM foo").assertSucceeded().assertDataResultMatch([(500,)])
            tc.execute("SELECT table, rowcount FROM persist_unlogged(\'put\', \'foo\')").assertSucceeded().assertDataResultMatch([('foo', 0)])

            # Simulate some work in order to trigger WAL flush(note that Mtests runs with --forcemito)
            tc.execute("CREATE TABLE bar (x INT)").assertSucceeded()
            tc.execute("INSERT INTO bar SELECT * FROM generate_series(0,100000)").assertSucceeded()

            tc.execute("SELECT table, rowcount FROM persist_unlogged(\'put\', \'foo\')").assertSucceeded().assertDataResultMatch([('foo', 500)])
        s.communicate()

    with process.server(mapiport='0', dbname='db1',
                        dbfarm=os.path.join(farm_dir, 'db1'),
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=s.dbport, database='db1')
            tc.execute("SET SCHEMA put").assertSucceeded()
            tc.execute("SELECT COUNT(*) FROM foo").assertSucceeded().assertDataResultMatch([(500,)])
        s.communicate()
