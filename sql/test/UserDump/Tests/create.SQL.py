from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    tc.execute("""
        create user voc with password 'voc' name 'VOC Explorer' schema sys;
        create schema voc authorization voc;
        alter user voc set schema voc;
            """, client="mclient").assertSucceeded()
    tc.connect(username='voc', password='voc')
    tc.execute("""
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
            """, client="mclient").assertSucceeded()
    tc.connect(username='monetdb', password='monetdb')
    tc.execute("""
        create user test with password 'test' name 'Test User' schema sys;
        create schema test authorization test;
        alter user test set schema test;
            """, client="mclient").assertSucceeded()
    tc.connect(username='test', password='test')
    tc.execute("""
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
        create trigger x after insert on foo insert into a values (1);
        create trigger "z" after insert on "foo" insert into a values (1);
            """, client="mclient").assertSucceeded()
