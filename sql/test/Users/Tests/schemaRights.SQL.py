###
# Grant SYSADMIN role to a user.
# Verify that the user can CREATE and DROP SCHEMA.
# Verify that the user can DROP SCHEMA created by both the 'monetdb' user and the created user.
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
CREATE USER user1 WITH PASSWORD 'user1' name 'schema test user1' schema sys;
CREATE USER user2 WITH PASSWORD 'user2' name 'schema test user2' schema sys;
GRANT sysadmin TO user1;
GRANT sysadmin TO user2;
CREATE SCHEMA schema1 AUTHORIZATION sysadmin;
""")



sql_test_client('user1', 'user1', input = """\
SET ROLE sysadmin;
DROP SCHEMA schema1;
CREATE SCHEMA schema2;
DROP SCHEMA schema2;
CREATE SCHEMA schema3;
""")


sql_test_client('user2', 'user2', input = """\
SET ROLE sysadmin;
DROP SCHEMA schema3;
""")


