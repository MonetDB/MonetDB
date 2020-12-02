###
# Let any user grant any role (not possible).
###

from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("""
        CREATE SCHEMA s1;
        CREATE USER bruce WITH PASSWORD 'bruce' name 'willis' schema s1;
        CREATE TABLE s1.test(d int);
        CREATE ROLE role1;
        GRANT ALL ON s1.test to role1;
     """).assertSucceeded()

    tc.connect(username="bruce", password="bruce")
    tc.execute("""
        GRANT role1 to bruce;
        SET role role1;
        select * from test;
     """).assertFailed()


# import os, sys
# try:
#     from MonetDBtesting import process
# except ImportError:
#     import process

# def sql_test_client(user, passwd, input):
#     with process.client(lang="sql", user=user, passwd=passwd, communicate=True,
#                         stdin=process.PIPE, stdout=process.PIPE, stderr=process.PIPE,
#                         input=input, port=int(os.getenv("MAPIPORT"))) as c:
#         c.communicate()

# sql_test_client('monetdb', 'monetdb', input="""\
# CREATE SCHEMA s1;
# CREATE USER bruce WITH PASSWORD 'bruce' name 'willis' schema s1;
# CREATE TABLE s1.test(d int);
# CREATE ROLE role1;
# GRANT ALL ON s1.test to role1;
# """)


# sql_test_client('bruce', 'bruce', input="""\
# GRANT role1 to bruce;
# SET role role1;
# select * from test;
# """)
