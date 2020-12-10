from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('sqlancer_prepare.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='sqlancer_prepare.stable.out')\
            .assertMatchStableError(ferr='sqlancer_prepare.stable.err')
