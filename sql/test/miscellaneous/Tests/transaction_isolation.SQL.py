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

        mdb1.execute("drop table integers;")
