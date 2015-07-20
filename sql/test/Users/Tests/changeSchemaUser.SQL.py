###
# Assess that the schema of a user can be changed.
# Assess that there is an error message if it tries to set an unexisting schema.
# Assess that there is an error message if it tries to set a schema for an unexisting user.
# Assess that a user that owns a schema cannot be dropped.
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
ALTER USER "april" SET SCHEMA library;
ALTER USER "april2" SET SCHEMA library; --no such user
ALTER USER "april" SET SCHEMA library2; --no such schema
""")

# This is the new april, so these operations should fail.
client('april', 'april', input = """\
SELECT * from bank.accounts; --no such table.
SELECT * from library.orders; --not enough privileges.
""")


client('monetdb', 'monetdb', input = """\
ALTER USER "april" SET SCHEMA bank;
CREATE SCHEMA forAlice AUTHORIZATION april;
DROP user april;
""")



