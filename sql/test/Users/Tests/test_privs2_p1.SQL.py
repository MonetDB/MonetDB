###
# Use functions that contain SELECT. INSERT, UPDATE, DELETE
# on a table for which the USER has GRANTs (possible).
###

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

clt = process.client('sql', user = 'my_user', passwd = 'p1',
                     stdin = open(os.path.join(os.getenv('RELSRCDIR'), os.pardir, 'test_privs2.sql')),
                     stdout = process.PIPE, stderr = process.PIPE)
out, err = clt.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
