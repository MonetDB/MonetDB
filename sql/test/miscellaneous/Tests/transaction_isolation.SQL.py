from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as mdb1:
    with SQLTestCase() as mdb2:
        mdb1.connect(username="monetdb", password="monetdb")
        mdb2.connect(username="monetdb", password="monetdb")
        mdb1.execute("CREATE TABLE integers (i int);").assertSucceeded()
        mdb1.execute("insert into integers values (1),(2),(3),(NULL);").assertSucceeded()

        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute('TRUNCATE integers;').assertRowCount(4)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute("insert into integers values (4),(5),(6);").assertRowCount(3)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(5,),(6,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute("update integers set i = 7 where i = 6;").assertRowCount(1)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(5,),(7,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute("delete from integers where i = 5;").assertRowCount(1)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(7,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute('rollback;').assertSucceeded()

        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute('TRUNCATE integers;').assertRowCount(4)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute("insert into integers values (4),(5),(6);").assertRowCount(3)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(5,),(6,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute("update integers set i = 7 where i = 6;").assertRowCount(1)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(5,),(7,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute("delete from integers where i = 5;").assertRowCount(1)
        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(7,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(1,),(2,),(3,),(None,)])
        mdb1.execute('commit;').assertSucceeded()

        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(7,)])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([(4,),(7,)])

        mdb1.execute('TRUNCATE integers;').assertRowCount(2)
        mdb1.execute("insert into integers (select value from generate_series(1,21,1));").assertRowCount(20) # 1 - 20

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute("delete from integers where i % 5 <> 0;").assertRowCount(16)
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(4,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(20,)])
        mdb1.execute("update integers set i = i + 1 where i % 2 = 0;").assertRowCount(2)
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(4,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(20,)])
        mdb1.execute("insert into integers (select value from generate_series(1,11,1));").assertRowCount(10)
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(14,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(20,)])
        mdb1.execute("delete from integers where i between 1 and 5;").assertRowCount(6)
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(8,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(20,)])
        mdb1.execute('TRUNCATE integers;').assertRowCount(8)
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(0,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(20,)])
        mdb1.execute('rollback;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute("insert into integers (select value from generate_series(1,21,1));").assertRowCount(20) # 1 - 20, 1 - 20

        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(40,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(40,)])

        mdb1.execute("insert into integers (select value from generate_series(1,201,1));").assertRowCount(200) # 1 - 20, 1 - 20, 1 - 200
        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute("delete from integers where i < 21;").assertRowCount(60) # 21 - 200
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(180,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(240,)])
        mdb1.execute("insert into integers (select value from generate_series(11,301,1));").assertRowCount(290) # 21 - 200, 11 - 300
        mdb1.execute('commit;').assertSucceeded()

        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb1.execute("delete from integers where i < 101;").assertRowCount(170) # 101 - 200, 101 - 300
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(300,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb1.execute("insert into integers (select value from generate_series(41,161,1));").assertRowCount(120) # 101 - 200, 101 - 300, 41 - 160
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(420,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb1.execute("delete from integers where i between 91 and 120;").assertRowCount(70) # 121 - 200, 121 - 300, 41 - 90, 121 - 160
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(350,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])

        mdb1.execute("delete from integers where i between 131 and 140 or i < 91;").assertRowCount(80) # 121 - 130, 141 - 200, 121 - 130, 141 - 300, 121 - 130, 141 - 160
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(270,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb1.execute("insert into integers (select value from generate_series(41,51,1));").assertRowCount(10) # 121 - 130, 141 - 200, 121 - 130, 141 - 300, 121 - 130, 141 - 160, 41 - 50
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(280,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb1.execute("delete from integers where i > 99;").assertRowCount(270) # 41 - 50
        mdb1.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb2.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(470,)])
        mdb1.execute('commit;').assertSucceeded()

        mdb1.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])

        mdb1.execute('start transaction;').assertSucceeded()
        mdb1.execute('TRUNCATE integers;').assertRowCount(10)
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(0,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb1.execute("insert into integers (select value from generate_series(1,101,1));").assertRowCount(100) # 1 - 100
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(100,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb1.execute("insert into integers (select value from generate_series(1,31,1));").assertRowCount(30) # 1 - 100, 1 - 30
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(130,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb1.execute('DELETE FROM integers WHERE i between 11 and 20;').assertRowCount(20) # 1 - 10, 21 - 100, 1 - 10, 21 - 30
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(110,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb1.execute('DELETE FROM integers WHERE i between 1 and 10 or i between 91 and 100;').assertRowCount(30) # 21 - 90, 21 - 30
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(80,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb1.execute("insert into integers (select value from generate_series(1,11,1));").assertRowCount(10) # 21 - 90, 21 - 30, 1 - 10
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(90,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb1.execute("TRUNCATE integers;").assertRowCount(90)
        mdb1.execute('SELECT count(*) FROM integers;').assertDataResultMatch([(0,)])
        mdb2.execute('SELECT i FROM integers order by i;').assertDataResultMatch([(41,),(42,),(43,),(44,),(45,),(46,),(47,),(48,),(49,),(50,)])
        mdb1.execute('commit;').assertSucceeded()

        mdb1.execute('SELECT i FROM integers;').assertDataResultMatch([])
        mdb2.execute('SELECT i FROM integers;').assertDataResultMatch([])

        mdb1.execute('insert into integers (select value from generate_series(1,11,1));').assertRowCount(10)
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('delete from integers where i = 10;').assertRowCount(1)
        mdb2.execute('delete from integers where i = 10;').assertFailed(err_code="42000", err_message="Delete failed due to conflict with another transaction")
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('update integers set i = 2 where i = 1;').assertRowCount(1)
        mdb2.execute('update integers set i = 2 where i = 1;').assertFailed(err_code="42000", err_message="Update failed due to conflict with another transaction")
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('delete from integers where i = 9;').assertRowCount(1)
        mdb2.execute('truncate integers;').assertFailed(err_code="42000", err_message="Table clear failed due to conflict with another transaction")
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute('alter table integers add column j int;').assertSucceeded()
        mdb1.execute('start transaction;').assertSucceeded()
        mdb2.execute('start transaction;').assertSucceeded()
        mdb1.execute('alter table integers drop column j;').assertSucceeded()
        mdb2.execute('alter table integers drop column j;').assertFailed(err_code="42000", err_message="ALTER TABLE: transaction conflict detected")
        mdb1.execute('commit;').assertSucceeded()
        mdb2.execute('rollback;').assertSucceeded()

        mdb1.execute("drop table integers;")
