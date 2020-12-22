from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepare_decimal_float.SF-1675795.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout='prepare_decimal_float.SF-1675795.stable.out')

