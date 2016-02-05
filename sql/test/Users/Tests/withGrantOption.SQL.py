###
# Grant a user the right to regrant a privilege.
# Verify that the user can regrant the privilege.
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
GRANT SELECT ON bank.loans TO april WITH GRANT OPTION;
GRANT INSERT ON bank.loans TO april WITH GRANT OPTION;
GRANT UPDATE ON bank.loans TO april WITH GRANT OPTION;
GRANT DELETE ON bank.loans TO april WITH GRANT OPTION;
""")


sql_test_client('april', 'april', input = """\
GRANT SELECT ON bank.loans TO alice WITH GRANT OPTION;
GRANT INSERT ON bank.loans TO alice WITH GRANT OPTION;
GRANT UPDATE ON bank.loans TO alice WITH GRANT OPTION;
GRANT DELETE ON bank.loans TO alice WITH GRANT OPTION;
""")


