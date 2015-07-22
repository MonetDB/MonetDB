###
# Let any user grant any role (not possible).
###

from util import sql_test_client

sql_client = os.getenv('SQL_CLIENT')

client('monetdb', 'monetdb', input = """\
CREATE SCHEMA s1;
CREATE USER bruce WITH PASSWORD 'bruce' name 'willis' schema s1;
CREATE TABLE s1.test(d int);
CREATE ROLE role1;
GRANT ALL ON s1.test to role1;
""")


client('bruce', 'bruce', input = """\
GRANT role1 to bruce;
SET role role1;
select * from test;
""")
