import sys
from MonetDBtesting.sqltest import SQLTestCase

with SQLTestCase() as tc:
    # TODO match on the output
    tc.execute('prepare select 1;', client='mclient').assertSucceeded()
    tc.execute('declare a int;').assertFailed(err_code='42000')
    tc.execute('select a;').assertFailed(err_code='42000')
    tc.execute('iamerror;').assertFailed(err_code='42000')
    tc.execute('select a;').assertFailed(err_code='42000')

