###
# Grant monetdb rights to a user.
# Verify that the user can assume the monetdb role and CREATE new users, GRANT privileges and roles.
###

from util import sql_test_client

sql_test_client('monetdb', 'monetdb', input = """\
GRANT sysadmin TO alice;
""")


sql_test_client('alice', 'alice', input = """\
SET ROLE sysadmin;
CREATE USER may WITH PASSWORD 'may' NAME 'May' SCHEMA library;
GRANT ALL ON orders TO april;
GRANT sysadmin TO april;
""")


