from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('subquery_prepare.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='subquery_prepare.stable.out')\
            .assertMatchStableError(ferr='subquery_prepare.stable.err')
