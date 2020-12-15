from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepare-where.SF-1238867.1238959.1238965.1240124.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='prepare-where.SF-1238867.1238959.1238965.1240124.stable.out')\
            .assertMatchStableError(ferr='prepare-where.SF-1238867.1238959.1238965.1240124.stable.err')


