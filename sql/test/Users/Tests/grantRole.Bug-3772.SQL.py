###
# Let any user grant any role (not possible).
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
CREATE SCHEMA s1;
CREATE USER bruce WITH PASSWORD 'bruce' name 'willis' schema s1;
CREATE TABLE s1.test(d int);
CREATE ROLE role1;
GRANT ALL ON s1.test to role1;
""")


sql_test_client('bruce', 'bruce', input = """\
GRANT role1 to bruce;
SET role role1;
select * from test;
""")
