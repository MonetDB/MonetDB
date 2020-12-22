###
# Check that a non-existing user cannot log in.
# Check that granting schema, role and privileges to a non-existing user is not
#   possible.
###

from MonetDBtesting.sqltest import SQLTestCase
import logging

logging.basicConfig(level=logging.FATAL)

usr = "this_user_does_not_exist"

with SQLTestCase() as tc:
    tc.connect(username=usr, password=usr)
    tc.execute("select 1;").assertFailed(err_code=None, err_message="InvalidCredentialsException:checkCredentials:invalid credentials for user '"+usr+"'")

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
        CREATE ROLE r1;
        CREATE TABLE t (i INT);
    """).assertSucceeded()

    tc.execute("GRANT r1 TO "+usr).assertFailed(err_code="M1M05", err_message="GRANT: no such role 'r1' or grantee '"+usr+"'")
    tc.execute("GRANT ALL ON t TO "+usr).assertFailed(err_code="01007", err_message="GRANT: User/role '"+usr+"' unknown")
    tc.execute("GRANT COPY INTO TO "+usr).assertFailed(err_code="01007", err_message="GRANT: User/role '"+usr+"' unknown")
    tc.execute("GRANT COPY FROM TO "+usr).assertFailed(err_code="01007", err_message="GRANT: User/role '"+usr+"' unknown")

    tc.execute("DROP TABLE t;").assertSucceeded()
    tc.execute("DROP ROLE r1;").assertSucceeded()
