import os, sys
from MonetDBtesting import process

c = process.client('sql',
                   args = [os.path.join(os.getenv('TSTSRCBASE'),
                                        os.getenv('TSTDIR'),
                                        'like_tests.sql')],
                   stdout = process.PIPE, stderr = process.PIPE)
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)
