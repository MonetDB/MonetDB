from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepared-merge-statement.Bug-6706.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='prepared-merge-statement.Bug-6706.stable.out')\
            .assertMatchStableError(ferr='prepared-merge-statement.Bug-6706.stable.err')
