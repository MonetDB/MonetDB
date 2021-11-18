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

        mdb1.execute('create merge table parent2(a int, b int);').assertSucceeded()
        mdb1.execute('create table child2(a int, b int);').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("drop table child2;").assertSucceeded()
        mdb2.execute("ALTER TABLE parent2 ADD TABLE child2;").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('create merge table parent3(a int, b int);').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("drop table parent3;").assertSucceeded()
        mdb2.execute("ALTER TABLE parent3 ADD TABLE parent2;").assertFailed(err_code="42000", err_message="ALTER TABLE: transaction conflict detected")
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute('CREATE ROLE myrole;').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('CREATE schema mysch AUTHORIZATION myrole;').assertSucceeded()
        mdb2.execute('DROP ROLE myrole;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table parent1 drop table child1;').assertSucceeded()
        mdb1.execute('drop table parent1;').assertSucceeded()
        mdb1.execute('drop table child1;').assertSucceeded()
        mdb1.execute('drop table parent2;').assertSucceeded()
        mdb1.execute('drop schema mysch;').assertSucceeded()
        mdb1.execute('drop role myrole;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()

with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('create merge table parent(a int, b int);').assertSucceeded()
        mdb1.execute('create table child1(a int, b int);').assertSucceeded()
        mdb1.execute("insert into child1 values (1,1);").assertSucceeded()
        mdb1.execute('create table child2(a int, b int);').assertSucceeded()
        mdb1.execute("insert into child2 values (2,2);").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("alter table parent add table child1;").assertSucceeded()
        mdb2.execute('alter table child1 add column data int;').assertSucceeded() # number of columns must match
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('select * from parent;').assertDataResultMatch([(1,1)])
        mdb2.execute('select * from parent;').assertDataResultMatch([(1,1)])

        mdb1.execute("alter table parent drop table child1;").assertSucceeded()
        mdb1.execute("alter table parent add table child2;").assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("alter table parent add table child1;").assertSucceeded()
        mdb2.execute('alter table child1 alter column a set not null;').assertSucceeded() # null constraints must match
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('select * from parent;').assertDataResultMatch([(1,1),(2,2)])
        mdb2.execute('select * from parent;').assertDataResultMatch([(1,1),(2,2)])

        mdb1.execute('alter table parent drop table child1;').assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("alter table parent add table child1;").assertSucceeded()
        mdb2.execute('alter table child1 drop column b;').assertSucceeded() # number of columns must match
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('select * from parent;').assertDataResultMatch([(1,1),(2,2)])
        mdb2.execute('select * from parent;').assertDataResultMatch([(1,1),(2,2)])

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table parent drop table child1;').assertSucceeded()
        mdb1.execute('alter table parent drop table child2;').assertSucceeded()
        mdb1.execute('drop table parent;').assertSucceeded()
        mdb1.execute('drop table child1;').assertSucceeded()
        mdb1.execute('drop table child2;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
