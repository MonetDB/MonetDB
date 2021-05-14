from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")
        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute("CREATE TABLE integers (i int);").assertSucceeded()
        mdb1.execute("insert into integers values (1),(2),(3),(NULL);").assertSucceeded()
        mdb1.execute("CREATE TABLE longs (i bigint);").assertSucceeded()
        mdb1.execute("insert into longs values (1),(2),(3);").assertSucceeded()
        mdb1.execute("insert into integers values (1),(2),(3);").assertSucceeded()
        mdb1.execute("alter table longs add primary key (i)").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table integers add foreign key(i) references longs(i);').assertSucceeded()
        mdb2.execute('alter table integers add foreign key(i) references longs(i);').assertFailed(err_code="42000", err_message="ALTER TABLE: sys_integers_integers_i_fkey conflicts with another transaction")
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute("drop table integers;").assertSucceeded()
        mdb1.execute("drop table longs;").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
