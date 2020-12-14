from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('round-or-truncate.Bug-6193.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout='round-or-truncate.Bug-6193.stable.out', ignore_headers=True)


