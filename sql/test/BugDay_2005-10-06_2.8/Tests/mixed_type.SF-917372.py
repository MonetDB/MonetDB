from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute("select 'a'+1;").assertFailed() # it could be either string failed to convert to 'lng' or 'hge'
    tc.execute("select 1-'a';").assertFailed() # it could be either string failed to convert to 'lng' or 'hge'
    tc.execute("select true+1;").assertSucceeded().assertDataResultMatch([(2,)])
