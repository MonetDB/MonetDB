import sys
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('fixed_limit_for_prepare.Bug-3208.sql') as f:
        tr = tc.execute(query=None, client='mclient', stdin=f)\
                .assertSucceeded()\
                .assertMatchStableOut('fixed_limit_for_prepare.Bug-3208.stable.out')

