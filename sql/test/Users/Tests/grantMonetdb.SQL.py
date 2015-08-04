###
# Grant monetdb rights to a user.
# Verify that the user can assume the monetdb role and CREATE new users, GRANT privileges and roles.
###

from util import sql_test_client

sql_test_client('monetdb', 'monetdb', input = """\
GRANT monetdb TO alice;
""")


sql_test_client('alice', 'alice', input = """\
SET ROLE monetdb;
CREATE USER may WITH PASSWORD 'may' NAME 'May' SCHEMA library;
GRANT ALL ON orders TO april;
GRANT monetdb TO april;
""")


