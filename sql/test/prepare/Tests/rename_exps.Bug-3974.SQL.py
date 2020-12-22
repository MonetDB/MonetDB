from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('rename_exps.Bug-3974.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout='rename_exps.Bug-3974.stable.out')


