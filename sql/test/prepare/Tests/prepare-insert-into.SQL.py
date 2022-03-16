from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect()
    with open('prepare-insert-into.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout='prepare-insert-into.stable.out')\

