import os, sys

from MonetDBtesting.sqltest import SQLTestCase
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(infile, user = 'monetdb', passwd = 'monetdb'):
    with process.client('sql', user=user, passwd=passwd, stdin=open(infile),
                        stdout=process.PIPE, stderr=process.PIPE) as clt:
        out, err = clt.communicate()
        sys.stdout.write(out)
        sys.stderr.write(err)

relsrcdir = os.getenv('RELSRCDIR')

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("create user \"user_test\" with password 'pass' NAME 'test_6_1_user' SCHEMA \"sys\";").assertSucceeded()
    tc.execute("create table t_6_1 (id int);").assertSucceeded()

with SQLTestCase() as tc:
    tc.connect(username="user_test", password="pass")
    tc.execute("create trigger test_6_1 after insert on t_6_1 insert into t_6_1 values(12);").assertFailed(err_message='CREATE TRIGGER: access denied for user_test to schema \'sys\'')
    tc.execute("create table t_6_2(age int);").assertFailed(err_message='CREATE TABLE: insufficient privileges for user \'user_test\' in schema \'sys\'')

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("create trigger test_6_2 after insert on t_6_2 insert into t_6_1 values(12);").assertFailed(err_message='CREATE TRIGGER: no such table \'t_6_2\'')
    tc.execute("drop trigger test_6_1;").assertFailed(err_message='DROP TRIGGER: no such trigger \'test_6_1\'')
    tc.execute("drop trigger test_6_2;").assertFailed(err_message='DROP TRIGGER: no such trigger \'test_6_2\'')
    tc.execute("drop table t_6_1;").assertSucceeded()
    tc.execute("drop table t_6_2;").assertFailed(err_message='DROP TABLE: no such table \'t_6_2\'')
    tc.execute("drop user user_test;").assertSucceeded()
