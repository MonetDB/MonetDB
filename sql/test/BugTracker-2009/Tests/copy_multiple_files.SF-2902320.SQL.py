from MonetDBtesting.sqltest import SQLTestCase
import platform

if platform.system() == 'Windows':
    suffix = '.Windows'
else:
    suffix = ''

with SQLTestCase() as tc:
    # optional or default connection
    tc.connect()
    with open('copy_multiple_files.SF-2902320.sql') as f:
        tc.execute(query=None, client='mclient', stdin=f)\
            .assertMatchStableOut(fout='copy_multiple_files.SF-2902320.stable.out%s' % (suffix))\
            .assertMatchStableError(ferr='copy_multiple_files.SF-2902320.stable.err%s' % (suffix))
