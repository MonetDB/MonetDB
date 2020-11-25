from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("select 'a'+1;").assertFailed()
    tc.execute("select 1-'a';").assertFailed()
    tc.execute("select true+1;").assertSucceeded().assertDataResultMatch([(2,)])
