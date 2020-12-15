from MonetDBtesting.sqltest import SQLTestCase
import platform

if platform.system() == 'Windows':
    suffix = '.Windows'
else:
    suffix = ''

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('opt_sql_append.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='opt_sql_append.stable.out%s' % (suffix))\
            .assertMatchStableError(ferr='opt_sql_append.stable.err%s' % (suffix))
