###
# Assess that when the role of a user, who is currently logged in and has
#   assumed that role, has been revoked, the user immedately lose all
#   privileges associated with that role.
###

from MonetDBtesting.sqltest import SQLTestCase


with SQLTestCase() as tc1:
    with SQLTestCase() as tc2:
        tc1.connect(username="monetdb", password="monetdb")
        # Create a user, schema and role
        tc1.execute("""
        CREATE SCHEMA s1;
        CREATE USER bruce WITH PASSWORD 'bruce' name 'willis' schema s1;
        CREATE TABLE s1.test(d int);
        CREATE ROLE role1;
        GRANT ALL ON s1.test to role1;
        GRANT role1 TO bruce;""").assertSucceeded()

        # Login as `bruce` and use `role1`
        tc2.connect(username="bruce", password="bruce")
        tc2.execute('SET role role1;').assertSucceeded()
        tc2.execute('INSERT INTO test VALUES (24), (42);').assertSucceeded().assertRowCount(2)
        tc2.execute('UPDATE test SET d = 42 WHERE d <> 42;').assertSucceeded().assertRowCount(1)
        tc2.execute('DELETE FROM test WHERE d = 42;').assertSucceeded().assertRowCount(2)
        tc2.execute('SELECT * FROM test;').assertSucceeded().assertRowCount(0).assertDataResultMatch([])

        # Revoke `role1` from `bruce`
        tc1.execute('REVOKE role1 FROM bruce;').assertSucceeded()

        # `bruce` should not be able to access `test` again:
        tc2.execute('INSERT INTO test VALUES (24), (42);').assertSucceeded().assertRowCount(2)
        tc2.execute('UPDATE test SET d = 42 WHERE d <> 42;').assertSucceeded().assertRowCount(1)
        tc2.execute('DELETE FROM test WHERE d = 42;').assertSucceeded().assertRowCount(2)
        tc2.execute('SELECT * FROM test;').assertSucceeded().assertRowCount(0).assertDataResultMatch([])
        tc2.execute('SET ROLE role1; -- verifies role1 is gone').assertFailed(err_message="Role (role1) missing")

    tc1.execute("""
    DROP USER bruce;
    DROP SCHEMA s1 CASCADE;
    DROP ROLE role1""").assertSucceeded()
