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
                         stdout=process.PIPE, stderr=process.PIPE) as srv:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=srv.dbport, database='db1')
            tc.execute("call sys.shutdown(10);").assertSucceeded()
        srv.communicate()
