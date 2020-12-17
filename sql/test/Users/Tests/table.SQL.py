###
# Check that if and only if a schema owner has been GRANTed the proper ROLE,
#   this use can CREATE, INS/UPD/DEL/SEL and DROP a table in this schema.
# Also check that a table created by a user can be dropped by `monetdb`
###
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb:
    mdb.connect(username="monetdb", password="monetdb")

    mdb.execute("CREATE ROLE my_role;").assertSucceeded()
    mdb.execute("CREATE SCHEMA my_schema AUTHORIZATION my_role;").assertSucceeded()
    mdb.execute("CREATE USER my_user with password 'p1' name 'User with role' schema my_schema;").assertSucceeded()

    with SQLTestCase() as usr:
        usr.connect(username="my_user", password="p1")
        usr.execute("""
            CREATE TABLE my_schema.my_table (
              obsid INT NOT NULL,
              time_s BIGINT NULL,
              time_e BIGINT NULL,
              PRIMARY KEY (obsid));
        """).assertFailed(err_code="42000", err_message="CREATE TABLE: insufficient privileges for user 'my_user' in schema 'my_schema'")

        mdb.execute("GRANT my_role to my_user;").assertSucceeded()
        usr.execute("SET ROLE my_role;").assertSucceeded()
        usr.execute("""
            CREATE TABLE my_schema.my_table (
              obsid INT NOT NULL,
              time_s BIGINT NULL,
              time_e BIGINT NULL,
              PRIMARY KEY (obsid));
        """).assertSucceeded()
        usr.execute("INSERT INTO my_schema.my_table (obsid, time_s) values (1, 300), (2, 42);")\
            .assertSucceeded()\
            .assertRowCount(2)
        usr.execute("UPDATE my_schema.my_table SET time_e = 999999 WHERE time_e IS NULL;")\
            .assertSucceeded()\
            .assertRowCount(2)
        usr.execute("DELETE FROM my_schema.my_table WHERE obsid = 1;")\
            .assertSucceeded()\
            .assertRowCount(1)
        usr.execute("SELECT * FROM my_schema.my_table;").assertSucceeded()\
            .assertDataResultMatch([(2, 42, 999999)])
        usr.execute("DROP TABLE my_schema.my_table;").assertSucceeded()

        usr.execute("""
            CREATE TABLE my_schema.my_table (
              obsid INT NOT NULL,
              time_s BIGINT NULL,
              time_e BIGINT NULL,
              PRIMARY KEY (obsid));
        """).assertSucceeded()

    mdb.execute("DROP TABLE my_schema.my_table;").assertSucceeded()
    mdb.execute("DROP USER my_user;").assertSucceeded()
    mdb.execute("DROP ROLE my_role;").assertSucceeded()
    mdb.execute("DROP SCHEMA my_schema;").assertSucceeded()
