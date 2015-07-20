###
# Revoke role and confirm user can no longer assume it.
# Assess there is an error message if dropping unexisting role.
# Assess it is not possible to recreate an existing role.
# Assess that only a user with the right privileges can grant a role to another role.
###

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(user, passwd, input=None):
    clt = process.client(lang='sql', user=user, passwd=passwd,
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE,
                         port = int(os.getenv('MAPIPORT')))
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

sql_client = os.getenv('SQL_CLIENT')

client('monetdb', 'monetdb', input = """\
REVOKE bankAdmin from april;
CREATE ROLE bankAdmin2;
GRANT bankAdmin2 to april;
DROP ROLE bankAdmin2;
DROP ROLE bankAdmin3; -- role doesn't exist
""")

client('april', 'april', input = """\
SET ROLE bankAdmin; -- role revoked
SET ROLE bankAdmin2; -- role no longer exists
""")
