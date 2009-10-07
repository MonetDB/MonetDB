import os, sys
import copy
try:
    import subprocess
except ImportError:
    # use private copy for old Python versions
    import MonetDBtesting.subprocess26 as subprocess

def client(cmd, input = None, env = os.environ):
    clt = subprocess.Popen(cmd,
                           env = env,
                           shell = True,
                           stdin = subprocess.PIPE,
                           stdout = subprocess.PIPE,
                           stderr = subprocess.PIPE,
                           universal_newlines = True)
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

def main():
    sql_client = os.getenv('SQL_CLIENT')
    client(sql_client, """\
create user voc with password 'voc' name 'VOC Explorer' schema sys;
create schema voc authorization voc;
alter user voc set schema voc;
""")
    vocenv = copy.deepcopy(os.environ)
    vocenv['DOTMONETDBFILE'] = '.vocuser'
    f = open(vocenv['DOTMONETDBFILE'], 'wb')
    f.write('user=voc\npassword=voc\n')
    f.close()
    client(sql_client, """\
create table foo (
        id int,
        v int,
        primary key (id)
);
create view bar as select * from foo;
create function b(i int) returns int begin return select v from bar where id = i; end;
create table a (
        id int
);
create trigger a after insert on foo insert into a values (1);
""", vocenv)
    client(sql_client, """\
create user test with password 'test' name 'Test User' schema sys;
create schema test authorization test;
alter user test set schema test;
""")
    testenv = copy.deepcopy(os.environ)
    testenv['DOTMONETDBFILE'] = '.testuser'
    f = open(testenv['DOTMONETDBFILE'], 'wb')
    f.write('user=test\npassword=test\n')
    f.close()
    client(sql_client, '''\
create table foo (
        id int,
        v int,
        primary key (id)
);
create view bar as select * from foo;
create function b(i int) returns int begin return select v from bar where id = i; end;
create table a (
        id int
);
create trigger a after insert on foo insert into a values (1);
create trigger test.x after insert on foo insert into a values (1);
create trigger "test"."z" after insert on "foo" insert into a values (1);
''', testenv)
    client(os.getenv('SQLDUMP'))
    client(os.getenv('SQLDUMP'), env = vocenv)
    client(os.getenv('SQLDUMP'), env = testenv)
    os.unlink(vocenv['DOTMONETDBFILE'])
    os.unlink(testenv['DOTMONETDBFILE'])

main()
