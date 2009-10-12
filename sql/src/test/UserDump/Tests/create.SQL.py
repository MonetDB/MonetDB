import os, sys
import subprocess

def client(cmd, input = None):
    clt = subprocess.Popen(cmd,
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
    client('%s -uvoc -Pvoc' % sql_client, """\
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
""")
    client(sql_client, """\
create user test with password 'test' name 'Test User' schema sys;
create schema test authorization test;
alter user test set schema test;
""")
    client('%s -utest -Ptest' % sql_client, '''\
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
''')
    client(os.getenv('SQLDUMP'))
    client('%s -uvoc -Pvoc' % os.getenv('SQLDUMP'))
    client('%s -utest -Ptest' % os.getenv('SQLDUMP'))

main()
