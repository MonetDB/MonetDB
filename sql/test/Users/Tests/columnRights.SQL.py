###
# Grant SELECT and UPDATE to a user on two different columns.
# Verify the user can SELECT and UPDATE on the column he has permissions for.
# Verify that the user cannot SELECT nor UPDATE on the column it did not get permissions for.
###

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def sql_test_client(user, passwd, input):
    process.client(lang = "sql", user = user, passwd = passwd, communicate = True,
                   stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE,
                   input = input, port = int(os.getenv("MAPIPORT")))

sql_test_client('monetdb', 'monetdb', input = """\
GRANT SELECT (price) ON library.orders TO alice;
GRANT UPDATE (name)  ON library.orders TO alice;
""")



sql_test_client('alice', 'alice', input = """\
SELECT price FROM orders;
UPDATE orders SET name = 'book title goes here';
SELECT name FROM orders; --insufficient rights
UPDATE orders SET price = 0; --insufficient rights
""")


