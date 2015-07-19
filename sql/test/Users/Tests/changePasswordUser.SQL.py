###
# Assess that the admin can change the password of a user.
# Assess that a user can change its one password.
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
ALTER USER april  WITH UNENCRYPTED PASSWORD 'april2';
""")

# try to log in with old password
client('april', 'april', input = """\
select 'password april';
""")

# try to log in with new password
client('april', 'april2', input = """\
select 'password april2';
ALTER USER SET UNENCRYPTED PASSWORD 'april5' USING OLD PASSWORD 'april3';
ALTER USER SET UNENCRYPTED PASSWORD 'april' USING OLD PASSWORD 'april2';
""")

# try to log in with old password
client('april', 'april2', input = """\
select 'password april2 (wrong!!!)';
""")


# try to log in with the new password
client('april', 'april', input = """\
select 'password change successfully';
""")



