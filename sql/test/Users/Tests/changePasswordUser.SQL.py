###
# Assess that the admin can change the password of a user.
# Assess that a user can change its own password.
###

from MonetDBtesting.sqltest import SQLTestCase
import logging

logging.basicConfig(level=logging.FATAL)

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("CREATE USER april WITH PASSWORD 'april' NAME 'april' SCHEMA sys;").assertSucceeded()
    tc.connect(username="april", password="april")
    tc.execute("select current_user, 'password is \"april\"';").assertSucceeded()\
            .assertDataResultMatch([("april", "password is \"april\"",)])

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("ALTER USER april WITH UNENCRYPTED PASSWORD 'april2';").assertSucceeded()
    tc.connect(username="april", password="april")
    tc.execute("select 'password incorrect april';").assertFailed(err_code=None, err_message="InvalidCredentialsException:checkCredentials:invalid credentials for user 'april'")
    tc.connect(username="april", password="april2")
    tc.execute("select 'password correct april2';").assertSucceeded()\
            .assertDataResultMatch([("password correct april2",)])
    # april tries to change its password with an incorrect old password
    tc.execute("ALTER USER SET UNENCRYPTED PASSWORD 'april5' USING OLD PASSWORD 'april3';")\
            .assertFailed(err_code="M0M27", err_message='ALTER USER: Access denied')
    tc.execute("ALTER USER SET UNENCRYPTED PASSWORD 'april' USING OLD PASSWORD 'april2';").assertSucceeded()
    tc.connect(username="april", password="april2")
    tc.execute("select 'password april2 (wrong!!!)';").assertFailed()
    tc.connect(username="april", password="april")
    tc.execute("select 'password change successfully';").assertSucceeded().assertDataResultMatch([("password change successfully",)])

    tc.connect(username="monetdb", password="monetdb")
    tc.execute("DROP USER april;").assertSucceeded()
