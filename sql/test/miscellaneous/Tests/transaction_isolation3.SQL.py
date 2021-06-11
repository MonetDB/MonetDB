from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")

        mdb1.execute("CREATE TABLE integers (i int, j int);").assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table integers add primary key (i);').assertSucceeded()
        mdb2.execute('insert into integers values (5,1),(5,2),(5,3);').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table integers alter j set not null;').assertSucceeded()
        mdb2.execute('insert into integers values (6,NULL),(7,NULL),(8,NULL);').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('create schema ups;').assertSucceeded()
        mdb1.execute('create merge table parent1(a int) PARTITION BY RANGE ON (a);').assertSucceeded()
        mdb1.execute('create table child1(c int);').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("ALTER TABLE parent1 ADD TABLE child1 AS PARTITION FROM '1' TO '2';").assertSucceeded() # these merge tables are very difficult, maybe allow only 1 transaction on the system?
        mdb2.execute("alter table child1 set schema ups;").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('create merge table parent2(a int) PARTITION BY RANGE ON (a);').assertSucceeded()
        mdb1.execute('create table child2(c int);').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("ALTER TABLE parent2 ADD TABLE child2 AS PARTITION FROM '1' TO '2';").assertSucceeded()
        mdb2.execute("insert into child2 values (3);").assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('commit;').assertFailed(err_code="40000", err_message="COMMIT: transaction is aborted because of concurrency conflicts, will ROLLBACK instead")

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('drop table integers;').assertSucceeded()
        mdb1.execute('drop schema ups;').assertSucceeded()
        mdb1.execute('ALTER TABLE parent1 DROP TABLE child1;').assertSucceeded()
        mdb1.execute('DROP TABLE parent1;').assertSucceeded()
        mdb1.execute('DROP TABLE child1;').assertSucceeded()
        mdb1.execute('ALTER TABLE parent2 DROP TABLE child2;').assertSucceeded()
        mdb1.execute('DROP TABLE parent2;').assertSucceeded()
        mdb1.execute('DROP TABLE child2;').assertSucceeded()
        mdb1.execute('commit;').assertSucceeded()
