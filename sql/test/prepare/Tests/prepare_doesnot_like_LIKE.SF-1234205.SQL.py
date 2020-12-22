from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepare_doesnot_like_LIKE.SF-1234205.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout='prepare_doesnot_like_LIKE.SF-1234205.stable.out')\



