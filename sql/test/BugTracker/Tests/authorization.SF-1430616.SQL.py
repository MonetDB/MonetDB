###
# Check that when a user is decoupled from a schema, this dependency is
#   properly removed and both the schema and the user can be dropped
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:

    # create the dependency of user `voc` on schema `voc`
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE USER voc WITH PASSWORD 'voc' NAME 'VOC Explorer' SCHEMA sys;").assertSucceeded()
    tc.execute("""
            SELECT users.name, users.fullname, schemas.name as schema
            FROM users, schemas
            WHERE users.default_schema = schemas.id
              AND users.name = 'voc';
        """).assertSucceeded()\
            .assertDataResultMatch([('voc', 'VOC Explorer', 'sys')])
    tc.execute("CREATE SCHEMA voc AUTHORIZATION voc;").assertSucceeded()
    tc.execute("ALTER USER voc SET SCHEMA voc;").assertSucceeded()
    tc.execute("""
            SELECT users.name, users.fullname, schemas.name as schema
            FROM users, schemas
            WHERE users.default_schema = schemas.id
              AND users.name = 'voc';
        """).assertSucceeded()\
            .assertDataResultMatch([('voc', 'VOC Explorer', 'voc')])

    # just use the new user once
    tc.connect(username="voc", password="voc")
    tc.execute("select current_schema;").assertSucceeded().assertDataResultMatch([('voc',)])

    tc.connect(username="monetdb", password="monetdb")
    # we can drop neither of them yet
    tc.execute("DROP SCHEMA voc;").assertFailed(err_code="2BM37", err_message="DROP SCHEMA: unable to drop schema 'voc' (there are database objects which depend on it)")
    tc.execute("DROP USER voc;").assertFailed(err_code="M1M05", err_message="DROP USER: 'voc' owns a schema")
    # remove the dependency so that we can drop both of them
    tc.execute("""
            START TRANSACTION;
            ALTER USER voc SET SCHEMA sys;
            DROP SCHEMA voc;
            DROP USER voc;
            COMMIT;
        """).assertSucceeded()

