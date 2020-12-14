from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    tc.drop()
    with open('decimal_needs_truncation.SF-2605686.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout='decimal_needs_truncation.SF-2605686.stable.out')
