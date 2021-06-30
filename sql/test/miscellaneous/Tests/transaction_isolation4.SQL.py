from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")

        mdb1.execute("create table myt (i int, j int);").assertSucceeded()
        mdb1.execute("insert into myt values (1, 1), (2, 2)").assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("alter table myt add constraint pk1 primary key (i);").assertSucceeded()
        mdb2.execute("alter table myt add constraint pk2 primary key (j);").assertFailed(err_code="42000", err_message="NOT NULL CONSTRAINT: transaction conflict detected") # only one pk per table
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute('CREATE schema mys;').assertSucceeded()
        mdb1.execute("CREATE ROLE myrole;").assertSucceeded()
        mdb1.execute("CREATE USER duser WITH PASSWORD 'ups' NAME 'ups' SCHEMA mys;").assertSucceeded()
        mdb1.execute("GRANT myrole to duser;").assertSucceeded()
        mdb1.execute("create table mys.myt2 (i int, j int);").assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("GRANT SELECT on table mys.myt2 to myrole;").assertSucceeded()
        mdb2.execute('drop role myrole;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("analyze sys.myt").assertSucceeded()
        mdb2.execute('drop table myt;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('comment on table "sys"."myt" is \'amifine?\';').assertSucceeded()
        mdb2.execute('drop table myt;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('CREATE schema mys2;').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("CREATE USER duser2 WITH PASSWORD 'ups' NAME 'ups' SCHEMA mys2;").assertSucceeded()
        mdb2.execute('drop schema mys2;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('drop table myt;').assertSucceeded()
        mdb1.execute('drop user duser;').assertSucceeded()
        mdb1.execute('drop role myrole;').assertSucceeded()
        mdb1.execute('drop schema mys cascade;').assertSucceeded()
        mdb1.execute('drop user duser2;').assertSucceeded()
        mdb1.execute('drop schema mys2;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
