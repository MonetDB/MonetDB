from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb1:
    mdb1.connect(username="monetdb", password="monetdb")
    mdb1.execute('START TRANSACTION;').assertSucceeded()
    mdb1.execute('SAVEPOINT mys;').assertSucceeded()
    mdb1.execute('CREATE SCHEMA ups;').assertSucceeded()
    mdb1.execute('SET SCHEMA ups;').assertSucceeded()
    mdb1.execute('ROLLBACK TO SAVEPOINT mys;').assertFailed(err_code="40000", err_message="ROLLBACK: finished successfully, but the session's schema could not be found on the current transaction")
    mdb1.execute('rollback;').assertFailed()

with SQLTestCase() as mdb1:
    mdb1.connect(username="monetdb", password="monetdb")
    mdb1.execute('START TRANSACTION;').assertSucceeded()
    mdb1.execute('SAVEPOINT mys2;').assertSucceeded()
    mdb1.execute('CREATE SCHEMA ups2;').assertSucceeded()
    mdb1.execute('SET SCHEMA ups2;').assertSucceeded()
    mdb1.execute('RELEASE SAVEPOINT mys2;').assertSucceeded()
    mdb1.execute('rollback;').assertFailed()

with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")

        mdb1.execute('create table child1(a int);').assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('create merge table parent1(a int);').assertSucceeded()
        mdb1.execute('alter table parent1 add table child1;').assertSucceeded()
        mdb2.execute("insert into child1 values (1);").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table parent1 drop table child1;').assertSucceeded()
        mdb1.execute('drop table parent1;').assertSucceeded()
        mdb1.execute('drop table child1;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
