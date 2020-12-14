import os
from MonetDBtesting.sqltest import SQLTestCase

stableout = 'prepare_statements_crash_server.Bug-2599.stable.out.int128' if os.getenv('HAVE_HGE') else 'prepare_statements_crash_server.Bug-2599.stable.out'

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepare_statements_crash_server.Bug-2599.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout=stableout)
