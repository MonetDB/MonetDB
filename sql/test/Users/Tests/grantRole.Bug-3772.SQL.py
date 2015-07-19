###
# Assess that a regular user cannot grant any role further.
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
