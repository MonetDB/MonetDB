###
# Let a schema owner inherit the rights of monetdb.
# Check that by assuming the sysadmin role the user has complete privileges (e.g. select, create, drop).
###

from MonetDBtesting.sqltest import SQLTestCase
with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE USER owner with password 'ThisIsAS3m1S3cur3P4ssw0rd' name 'user gets monetdb rights' schema sys;").assertSucceeded()
    tc.execute("CREATE SCHEMA schemaForOwner AUTHORIZATION owner;").assertSucceeded()
    tc.execute("CREATE table schemaForOwner.testTable(v1 int, v2 int);").assertSucceeded()
    tc.execute("GRANT sysadmin to owner;").assertSucceeded()

    tc.connect(username="owner", password="ThisIsAS3m1S3cur3P4ssw0rd")
    tc.execute("""
        set schema schemaForOwner;
        set role sysadmin;
        DROP TABLE testTable;
        CREATE TABLE testTable(v1 INT);
        ALTER TABLE testTable ADD COLUMN v2 INT;
        SELECT * FROM testTable;
        INSERT INTO testTable VALUES (3, 3);
        UPDATE testTable SET v1 = 2 WHERE v2 = 7;
        DELETE FROM testTable WHERE v1 = 2;
    """).assertSucceeded()

