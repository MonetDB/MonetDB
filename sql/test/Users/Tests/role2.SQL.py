###
# SET a non-GRANTed ROLE for a USER (not possible).
###

import os, sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

clt = process.client('sql', user = 'my_user2', passwd = 'p2',
                     stdin = open(os.path.join(os.getenv('RELSRCDIR'), os.pardir, 'role.sql')),
                     stdout = process.PIPE, stderr = process.PIPE)
out, err = clt.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
