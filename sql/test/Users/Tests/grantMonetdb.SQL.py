###
# Grant monetdb rights to a user.
# Verify that the user can assume the monetdb role and CREATE new users, GRANT privileges and roles.
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
GRANT sysadmin TO alice;
""")


sql_test_client('alice', 'alice', input = """\
SET ROLE sysadmin;
CREATE USER may WITH PASSWORD 'may' NAME 'May' SCHEMA library;
GRANT ALL ON orders TO april;
GRANT sysadmin TO april;
""")


