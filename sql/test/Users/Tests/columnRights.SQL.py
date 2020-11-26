###
# Grant SELECT and UPDATE to a user on two different columns.
# Verify the user can SELECT and UPDATE on the column he has permissions for.
# Verify that the user cannot SELECT nor UPDATE on the column it did not get permissions for.
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("GRANT SELECT (price) ON library.orders TO alice;").assertSucceeded()
    tc.execute("GRANT UPDATE (name)  ON library.orders TO alice;").assertSucceeded()

    tc.connect(username="alice", password="alice")
    tc.execute("SELECT price FROM library.orders;").assertSucceeded().assertRowCount(0).assertDataResultMatch([])
    tc.execute("UPDATE library.orders SET name = 'book title goes here';").assertSucceeded().assertRowCount(0)
    tc.execute("SELECT name FROM library.orders; --insufficient rights").assertFailed(err_message="SELECT: identifier 'name' unknown")
    tc.execute("UPDATE orders SET price = 0; --insufficient rights").assertFailed(err_message="UPDATE: insufficient privileges for user 'alice' to update table 'orders' on column 'price'")

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
# GRANT SELECT (price) ON library.orders TO alice;
# GRANT UPDATE (name)  ON library.orders TO alice;
# """)



# sql_test_client('alice', 'alice', input="""\
# SELECT price FROM orders;
# UPDATE orders SET name = 'book title goes here';
# SELECT name FROM orders; --insufficient rights
# UPDATE orders SET price = 0; --insufficient rights
# """)


