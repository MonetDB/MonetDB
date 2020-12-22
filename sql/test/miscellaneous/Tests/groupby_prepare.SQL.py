from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('groupby_prepare.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='groupby_prepare.stable.out')\
            .assertMatchStableError(ferr='groupby_prepare.stable.err')
