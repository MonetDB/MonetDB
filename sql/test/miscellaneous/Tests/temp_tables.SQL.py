from MonetDBtesting.sqltest import SQLTestCase

# another temp tables test case
with SQLTestCase() as mdb1:
    mdb1.connect(username="monetdb", password="monetdb")
    mdb1.execute("CREATE GLOBAL TEMPORARY TABLE t2(c0 INT, c1 TIME UNIQUE) ON COMMIT DELETE ROWS;").assertSucceeded()

with SQLTestCase() as mdb2:
    mdb2.connect(username="monetdb", password="monetdb")
    mdb2.execute("INSERT INTO tmp.t2(c1) VALUES(TIME '13:35:22');").assertSucceeded().assertRowCount(1)

with SQLTestCase() as mdb3:
    mdb3.connect(username="monetdb", password="monetdb")
    mdb3.execute("INSERT INTO tmp.t2(c1, c0) VALUES(TIME '13:41:34', 66);").assertSucceeded().assertRowCount(1)
    mdb3.execute("DROP TABLE tmp.t2;").assertSucceeded()
