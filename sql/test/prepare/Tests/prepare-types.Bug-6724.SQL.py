from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepare-types.Bug-6724.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='prepare-types.Bug-6724.stable.out')\
            .assertMatchStableError(ferr='prepare-types.Bug-6724.stable.err')
