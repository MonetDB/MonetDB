###
# Assess that the user can use a granted privilege without having to logout.
# Assess that a user can no longer use the privilege as soon as it was revoked.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc1:
    tc1.connect(username="monetdb", password="monetdb")
    tc1.execute('CREATE SCHEMA new_schema_as_well').assertSucceeded()
    tc1.execute('SET SCHEMA new_schema_as_well').assertSucceeded()
    tc1.execute('CREATE TABLE test (x int, y int)').assertSucceeded()
    tc1.execute('INSERT INTO test VALUES (-1, -1)').assertSucceeded()

     # Create a new user and grant the right to select.
    tc1.execute('CREATE USER new_user WITH PASSWORD \'new_quite_long_password\' NAME \'newUser\' SCHEMA new_schema_as_well').assertSucceeded()
    tc1.execute('GRANT SELECT ON new_schema_as_well.test TO new_user').assertSucceeded()
    tc1.execute('GRANT UPDATE ON new_schema_as_well.test TO new_user').assertSucceeded()
    tc1.execute('GRANT INSERT ON new_schema_as_well.test TO new_user').assertSucceeded()
    tc1.execute('GRANT DELETE ON new_schema_as_well.test TO new_user').assertSucceeded()

    with SQLTestCase() as tc2:
        # Login the new user, and select.
        tc2.connect(username='new_user', password='new_quite_long_password')
        tc2.execute('SELECT * FROM test').assertSucceeded()
        tc2.execute('UPDATE test SET x = -3 WHERE y = -1').assertSucceeded()
        tc2.execute('INSERT INTO test VALUES (0, 0)').assertSucceeded()
        tc2.execute('DELETE FROM test WHERE y = -2').assertSucceeded()

        # Revoke the right to select from the new user.
        tc1.execute('REVOKE SELECT ON new_schema_as_well.test FROM new_user').assertSucceeded()

        # The new user should not be able to select anymore while still logged in
        tc2.execute('SELECT * FROM test').assertFailed(err_code="42000", err_message="SELECT: access denied for new_user to table 'new_schema_as_well.test'")
        tc2.execute('UPDATE test SET x = -66 WHERE y = 66').assertFailed(err_code="42000", err_message="UPDATE: insufficient privileges for user 'new_user' to update table 'test'")
        tc2.execute('INSERT INTO test VALUES (66, 66)').assertSucceeded()
        tc2.execute('DELETE FROM test WHERE y = -66').assertFailed(err_code="42000", err_message="DELETE FROM: insufficient privileges for user 'new_user' to delete from table 'test'")

        # Re-grant the right to select to the new user.
        tc1.execute('GRANT SELECT ON new_schema_as_well.test TO new_user').assertSucceeded()

        # The new user should be able to select again while still logged in
        tc2.execute('SELECT * FROM test').assertSucceeded()
        tc2.execute('UPDATE test SET x = -66 WHERE y = 66').assertSucceeded()
        tc2.execute('INSERT INTO test VALUES (66, 66)').assertSucceeded()
        tc2.execute('DELETE FROM test WHERE y = -66').assertSucceeded()
