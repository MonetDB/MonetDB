from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute('create function myfunc() returns table (x int) begin declare myvar int; return select myvar; end;').assertSucceeded()
    tc.execute('create function myfunc2() returns int begin declare myvar int; return myvar; end;').assertSucceeded()
    tc.execute('select * from myfunc();').assertSucceeded().assertDataResultMatch([(None,)])
    tc.execute('select myfunc2();').assertSucceeded().assertDataResultMatch([(None,)])

with SQLTestCase() as tc:
    tc.connect(username="monetdb", password="monetdb")
    tc.execute('select * from myfunc();').assertSucceeded().assertDataResultMatch([(None,)])
    tc.execute('select myfunc2();').assertSucceeded().assertDataResultMatch([(None,)])
    tc.execute('drop function myfunc();').assertSucceeded()
    tc.execute('drop function myfunc2();').assertSucceeded()
