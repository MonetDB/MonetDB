###
# Assess that the admin can change the password of a user.
# Assess that a user can change its own password.
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
ALTER USER april  WITH UNENCRYPTED PASSWORD 'april2';
""")

# try to log in with old password
sql_test_client('april', 'april', input = """\
select 'password april';
""")

# try to log in with new password
sql_test_client('april', 'april2', input = """\
select 'password april2';
ALTER USER SET UNENCRYPTED PASSWORD 'april5' USING OLD PASSWORD 'april3';
ALTER USER SET UNENCRYPTED PASSWORD 'april' USING OLD PASSWORD 'april2';
""")

# try to log in with old password
sql_test_client('april', 'april2', input = """\
select 'password april2 (wrong!!!)';
""")


# try to log in with the new password
sql_test_client('april', 'april', input = """\
select 'password change successfully';
""")



