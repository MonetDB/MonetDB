from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepare_decimal_bug.SF-2831994.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='prepare_decimal_bug.SF-2831994.stable.out')\
            .assertMatchStableError(ferr='prepare_decimal_bug.SF-2831994.stable.err')
