from MonetDBtesting.sqltest import SQLTestCase

qry = "PREPARE SELECT id FROM tables WHERE name = LOWER(?);"
with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    tc.execute(qry, client='mclient')\
        .assertSucceeded()\
        .assertMatchStableOut(fout='prepare_unop_crash.Bug-3653.stable.out')


