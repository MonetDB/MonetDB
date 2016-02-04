###
# Revoke role and confirm user can no longer assume it.
# Assess there is an error message if dropping unexisting role.
# Assess it is not possible to recreate an existing role.
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
REVOKE bankAdmin from april;
CREATE ROLE bankAdmin2;
GRANT bankAdmin2 to april;
DROP ROLE bankAdmin2;
DROP ROLE bankAdmin3; -- role doesn't exist
""")

sql_test_client('april', 'april', input = """\
SET ROLE bankAdmin; -- role revoked
SET ROLE bankAdmin2; -- role no longer exists
""")
