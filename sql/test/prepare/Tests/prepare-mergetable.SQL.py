from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect()
    with open('prepare-mergetable.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout='prepare-mergetable.stable.out')\

