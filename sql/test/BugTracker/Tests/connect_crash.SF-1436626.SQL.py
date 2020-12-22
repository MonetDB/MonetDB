from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("select 1;").assertSucceeded().assertRowCount(1).assertDataResultMatch([(1,)])
