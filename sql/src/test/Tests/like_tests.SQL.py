import os
from MonetDBtesting import process

c = process.client('sql',
                   args = [os.path.join(os.getenv('TSTSRCBASE'),
                                        os.getenv('TSTDIR'),
                                        'like_tests.sql')],
                   stdin = process.PIPE)
c.communicate()
