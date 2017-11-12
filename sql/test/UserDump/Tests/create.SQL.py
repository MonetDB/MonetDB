import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

def client(lang, user = 'monetdb', passwd = 'monetdb', input = None):
    clt = process.client(lang, user = user, passwd = passwd,
                         stdin = process.PIPE,
                         stdout = process.PIPE,
                         stderr = process.PIPE)
    out, err = clt.communicate(input)
    sys.stdout.write(out)
    sys.stderr.write(err)

client('sql', input = """\
create user voc with password 'voc' name 'VOC Explorer' schema sys;
create schema voc authorization voc;
alter user voc set schema voc;
""")
client('sql', user = 'voc', passwd = 'voc', input = """\
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
client('sql', input = """\
create user test with password 'test' name 'Test User' schema sys;
create schema test authorization test;
alter user test set schema test;
""")
client('sql', user = 'test', passwd = 'test', input = '''\
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
client('sqldump')
client('sqldump', user = 'voc', passwd = 'voc')
client('sqldump', user = 'test', passwd = 'test')
