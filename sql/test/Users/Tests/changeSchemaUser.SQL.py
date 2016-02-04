###
# Change the default schema of a user (possible).
# Change the default schema of a user to an unexisting schema (not possible).
# Change the default schema for an unexisting user (not possible).
# Drop a user that owns a schema (not possible).
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
ALTER USER "april" SET SCHEMA library;
ALTER USER "april2" SET SCHEMA library; --no such user
ALTER USER "april" SET SCHEMA library2; --no such schema
""")

# This is the new april, so these operations should fail.
sql_test_client('april', 'april', input = """\
SELECT * from bank.accounts; --no such table.
SELECT * from library.orders; --not enough privileges.
""")


sql_test_client('monetdb', 'monetdb', input = """\
ALTER USER "april" SET SCHEMA bank;
CREATE SCHEMA forAlice AUTHORIZATION april;
DROP user april;
""")



