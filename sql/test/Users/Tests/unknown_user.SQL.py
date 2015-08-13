###
# Authenticate unknown USER (not possible).
###

import sys
try:
    from MonetDBtesting import process
except ImportError:
    import process

clt = process.client('sql',
                     user = 'this_user_does_not_exist',
                     passwd = 'this_password_does_not_exist',
                     stdout = process.PIPE, stderr = process.PIPE)
out, err = clt.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
