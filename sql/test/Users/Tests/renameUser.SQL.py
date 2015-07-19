###
# Assess that the admin can change the password of a user.
# Assess that a user can change its one password.
# Assess that by changing the username the user keeps their privileges.
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
ALTER USER "april" RENAME TO "april2"; --succeed
CREATE USER april with password 'april' name 'second' schema bank;
""")

# This is the new april, so these operations should fail.
client('april', 'april', input = """\
DELETE from bank.accounts; -- not enough privelges
SET role bankAdmin; -- no such role
ALTER USER "april2" RENAME TO "april3"; --not enough privileges
""")

# This is the initial april, so these operations should succeed.
client('april2', 'april', input = """\
SELECT * from bank.accounts;
SET role bankAdmin;
""")

client('monetdb', 'monetdb', input = """\
ALTER USER "april2" RENAME TO "april";
drop user april;
ALTER USER "april2" RENAME TO "april";
ALTER USER "april5" RENAME TO "april2"; -- no such user
drop user april2; --nu such user
CREATE USER april2 with password 'april' name 'second april, no rights' schema library2; --no such schema
CREATE USER april with password 'april' name 'second april, no rights' schema library; --user exsists
""")
