import os
from MonetDBtesting import process

c = process.client('sql',
                   args = [os.path.join(os.getenv('TSTSRCBASE'),
                                        os.getenv('TSTDIR'),
                                        'zones2.sql')],
                   stdin = process.PIPE)
c.communicate()
c = process.client('sql',
                   args = [os.path.join(os.getenv('TSTSRCBASE'),
                                        os.getenv('TSTDIR'),
                                        'zones2.sql')],
                   stdin = process.PIPE)
c.communicate()
