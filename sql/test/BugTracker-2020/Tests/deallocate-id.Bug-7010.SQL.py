from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    tc.connect()
    with open('deallocate-id.Bug-7010.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='deallocate-id.Bug-7010.stable.out')\
            .assertMatchStableError(ferr='deallocate-id.Bug-7010.stable.err')
