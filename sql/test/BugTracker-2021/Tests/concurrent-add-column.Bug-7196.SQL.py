from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")

        mdb1.execute('create table test (id bigint);').assertSucceeded()
        mdb1.execute("insert into test values (1);").assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table test add column data int;').assertSucceeded()
        mdb2.execute("insert into test values (2);").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('select * from test;').assertDataResultMatch([(1,None)])
        mdb2.execute('select * from test;').assertDataResultMatch([(1,None)])

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table test drop column data;').assertSucceeded()
        mdb2.execute("insert into test values (3,4);").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('select * from test;').assertDataResultMatch([(1,),(3,)])
        mdb2.execute('select * from test;').assertDataResultMatch([(1,),(3,)])

        mdb1.execute('drop table test;').assertSucceeded()
