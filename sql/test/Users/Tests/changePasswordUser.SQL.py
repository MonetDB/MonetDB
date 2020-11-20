###
# Assess that the admin can change the password of a user.
# Assess that a user can change its own password.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("ALTER USER april  WITH UNENCRYPTED PASSWORD 'april2';").assertSucceeded()
    tc.connect(username="april", password="april")
    tc.execute("select 'password april';").assertFailed()
    tc.connect(username="april", password="april2")
    tc.execute("select 'password april2';").assertSucceeded()
    tc.execute("ALTER USER SET UNENCRYPTED PASSWORD 'april5' USING OLD PASSWORD 'april3';").assertFailed()
    tc.execute("ALTER USER SET UNENCRYPTED PASSWORD 'april' USING OLD PASSWORD 'april2';").assertSucceeded()
    tc.execute("select 'password april2 (wrong!!!)';").assertFailed()
    tc.connect(username="april", password="april")
    tc.execute("select 'password change successfully';").assertSucceeded()

# import os, sys
# try:
#     from MonetDBtesting import process
# except ImportError:
#     import process

# def sql_test_client(user, passwd, input):
#     with process.client(lang="sql", user=user, passwd=passwd, communicate=True,
#                         stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE,
#                         input=input, port=int(os.getenv("MAPIPORT"))) as c:
#         c.communicate()

# sql_test_client('monetdb', 'monetdb', input="""\
# ALTER USER april  WITH UNENCRYPTED PASSWORD 'april2';
# """)

# # try to log in with old password
# sql_test_client('april', 'april', input="""\
# select 'password april';
# """)

# # try to log in with new password
# sql_test_client('april', 'april2', input="""\
# select 'password april2';
# ALTER USER SET UNENCRYPTED PASSWORD 'april5' USING OLD PASSWORD 'april3';
# ALTER USER SET UNENCRYPTED PASSWORD 'april' USING OLD PASSWORD 'april2';
# """)

# # try to log in with old password
# sql_test_client('april', 'april2', input="""\
# select 'password april2 (wrong!!!)';
# """)


# # try to log in with the new password
# sql_test_client('april', 'april', input="""\
# select 'password change successfully';
# """)



