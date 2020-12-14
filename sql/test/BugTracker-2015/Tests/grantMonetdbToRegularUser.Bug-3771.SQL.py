###
# Let a user who is not the owner of schema inherit the rights of monetdb.
# Check that by assuming the sysadmin role the user has complete privileges (e.g. select, create, drop).
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE USER user_with_many_rights with password 'ThisIsAS3m1S3cur3P4ssw0rd' name 'user gets monetdb rights' schema sys;").assertSucceeded()
    tc.execute("CREATE SCHEMA a_brand_new_schema_with_a_longer_name_than_usual;").assertSucceeded()
    tc.execute("CREATE table a_brand_new_schema_with_a_longer_name_than_usual.testTable(v1 int, v2 int);").assertSucceeded()
    tc.execute("GRANT sysadmin to user_with_many_rights;").assertSucceeded()

    tc.connect(username="user_with_many_rights", password="ThisIsAS3m1S3cur3P4ssw0rd")
    tc.execute("""
            set schema a_brand_new_schema_with_a_longer_name_than_usual;
            set role sysadmin;
            DROP TABLE testTable;
            CREATE TABLE testTable(v1 INT);
            ALTER TABLE testTable ADD COLUMN v2 INT;
            SELECT * FROM testTable;
            INSERT INTO testTable VALUES (3, 3);
            UPDATE testTable SET v1 = 2 WHERE v2 = 7;
            DELETE FROM testTable WHERE v1 = 2;
        """).assertSucceeded()

