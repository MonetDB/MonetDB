import os, sys, tempfile
from MonetDBtesting.sqltest import SQLTestCase
try:
    from MonetDBtesting import process
except ImportError:
    import process

class server_start(process.server):
    def __init__(self):
        super().__init__(mapiport='0', dbname='db1',
                         dbfarm=os.path.join(farm_dir, 'db1'),
                         stdin=process.PIPE, stdout=process.PIPE,
                         stderr=process.PIPE)

with tempfile.TemporaryDirectory() as farm_dir:
    os.mkdir(os.path.join(farm_dir, 'db1'))

    with server_start() as srv:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=srv.dbport, database='db1')
            tc.execute("""
            CREATE USER "voc2" WITH PASSWORD 'voc2' NAME 'VOC_EXPLORER' SCHEMA "sys";
            CREATE SCHEMA "voc2" AUTHORIZATION "voc2";
            ALTER USER "voc2" SET SCHEMA "voc2";
            alter user "voc2" with password 'new';
            """).assertSucceeded()
        srv.communicate()

    with server_start() as srv:
        with SQLTestCase() as tc:
            tc.connect(username="voc2", password="new", port=srv.dbport, database='db1')
            tc.execute("""
            select 1;
            """).assertSucceeded().assertRowCount(1).assertDataResultMatch([(1,)])
        srv.communicate()

    with server_start() as srv:
        with SQLTestCase() as tc:
            tc.connect(username="monetdb", password="monetdb", port=srv.dbport, database='db1')
            tc.execute("DROP SCHEMA \"voc2\";").assertFailed(err_message='DROP SCHEMA: unable to drop schema \'voc2\' (there are database users using it as session\'s default schema)')
            tc.execute("""
            ALTER user "voc2" SET SCHEMA "sys";
            DROP SCHEMA "voc2";
            DROP USER "voc2";
            """).assertSucceeded()
        srv.communicate()
