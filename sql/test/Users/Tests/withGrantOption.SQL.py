###
# Grant a user the right to regrant a privilege.
# Verify that the user can regrant the privilege.
###

from util import sql_test_client

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


