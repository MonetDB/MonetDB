from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect()
    with open('insert-prepare.Bug-7230.sql') as f:
        tr = tc.execute(query=None, client='mclient', stdin=f)\
                .assertSucceeded()\
                .assertMatchStableOut('insert-prepare.Bug-7230.stable.out')
