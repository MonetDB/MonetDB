import os
from MonetDBtesting.sqltest import SQLTestCase

stableout = 'prepare-smallint.Bug-3297.stable.out.int128' if os.getenv('HAVE_HGE') else 'prepare-smallint.Bug-3297.stable.out'

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('prepare-smallint.Bug-3297.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertSucceeded()\
            .assertMatchStableOut(fout=stableout)

