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
                        args=["--set", "insertonly_nowal=true", "--set", "embedded_py=true"],
                        stdin=process.PIPE,
                        stdout=process.PIPE, stderr=process.PIPE) as s:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=s.dbport, database='db1')
            tc.execute("CREATE OR REPLACE FUNCTION sleep(msecs int) RETURNS INT EXTERNAL NAME alarm.sleep")
            tc.execute("CREATE TABLE foo (x INT)").assertSucceeded()
            tc.execute("ALTER TABLE foo SET INSERT ONLY").assertSucceeded()
            tc.execute("CREATE LOADER up() LANGUAGE PYTHON { _emit.emit({'x': list(range(1,101))}) }").assertSucceeded()
            tc.execute("COPY LOADER INTO foo FROM up()").assertSucceeded()
            tc.execute("SELECT count(*) FROM foo").assertSucceeded().assertDataResultMatch([(100,)])
            tc.execute("select sleep(5000)")
            tc.execute("SELECT * FROM insertonly_persist('sys')").assertSucceeded()
        s.communicate()

        with process.server(mapiport='0', dbname='db1',
                            dbfarm=os.path.join(farm_dir, 'db1'),
                            stdin=process.PIPE,
                            stdout=process.PIPE, stderr=process.PIPE) as s:
            with SQLTestCase() as tc:
                tc.connect(username="monetdb", password="monetdb", port=s.dbport, database='db1')
                tc.execute("SELECT COUNT(*) FROM foo").assertSucceeded().assertDataResultMatch([(100,)])
            s.communicate()
