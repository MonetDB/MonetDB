import os
from MonetDBtesting import process

c = process.client('sql',
                   args = [os.path.join(os.getenv('TSTSRCBASE'),
                                        os.getenv('TSTDIR'),
                                        'null-byte-hang.sql')],
                   stdin = process.PIPE)
c.communicate()
