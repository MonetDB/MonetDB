import os, sys, tempfile
from MonetDBtesting.sqltest import SQLTestCase
try:
    from MonetDBtesting import process
except ImportError:
    import process

class server_start(process.server):
    def __init__(self, args=[]):
        super().__init__(mapiport='0', dbname='db1',
                         dbfarm=os.path.join(farm_dir, 'db1'),
                         stdin=process.PIPE, stdout=process.PIPE,
                         stderr=process.PIPE)

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with server_start(["--set", "sql_debug=64"]) as srv:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=srv.dbport, database='db1')
            tc.execute("""
            CREATE USER "skyserver" WITH PASSWORD 'skyserver' NAME 'sky server' SCHEMA
            "sys";
            create schema "sky" authorization "skyserver";
            alter user "skyserver" set schema "sky";
            """).assertSucceeded()
        srv.communicate()

    with server_start() as srv:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=srv.dbport, database='db1')
            tc.execute("""
            select u.name, u.fullname, s.name as default_schema from sys.users u, sys.schemas s where u.default_schema = s.id and u.name like '%skyserver%';
            """).assertSucceeded().assertRowCount(1).assertDataResultMatch([("skyserver","sky server","sky")])
            tc.execute("""
            alter user "skyserver" set schema "sys";
            drop schema sky;
            drop user skyserver;
            """).assertSucceeded()
        srv.communicate()
