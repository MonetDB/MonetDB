###
# Authenticate unknown USER (not possible).
###

from MonetDBtesting.sqltest import SQLTestCase
import logging

logging.basicConfig(level=logging.FATAL)

err_msg = "InvalidCredentialsException:checkCredentials:invalid credentials for user 'this_user_does_not_exist'".strip()

with SQLTestCase() as tc:
    tc.connect(username="this_user_does_not_exist", password="this_password_does_not_exist")
    tc.execute("SELECT * FROM sys;").assertFailed(err_message=err_msg)


# import sys,os
# import pymonetdb

# import logging

# logging.basicConfig(level=logging.FATAL)

# error = False
# db=os.getenv("TSTDB")
# port=int(os.getenv("MAPIPORT"))
# try:
#     client = pymonetdb.connect(database=db, port=port, autocommit=True,
#         user='this_user_does_not_exist', password='this_password_does_not_exist')
# except pymonetdb.exceptions.DatabaseError:
#     error = True
#     pass

# if not error:
#     print("Logged in with invalid credentials")
#     sys.exit(-1)
