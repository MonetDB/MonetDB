from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('ST_MakeBox2D.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='ST_MakeBox2D.stable.out', ignore_headers=True)\
            .assertMatchStableError(ferr='ST_MakeBox2D.stable.err')
