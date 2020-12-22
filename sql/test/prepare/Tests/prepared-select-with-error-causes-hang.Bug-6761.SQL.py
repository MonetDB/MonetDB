from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepared-select-with-error-causes-hang.Bug-6761.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='prepared-select-with-error-causes-hang.Bug-6761.stable.out')
